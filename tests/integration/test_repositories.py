from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.payment import PaymentStatus
from app.infra.exceptions import ConcurrentUpdateError
from app.infra.models import OutboxModel, PaymentModel
from tests.factories import make_payment
from tests.helpers import present
from tests.integration.conftest import RequestScope, ScopeFactory


async def count(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return (await session.execute(select(func.count()).select_from(model))).scalar_one()


class TestPaymentRepositories:
    async def test_add_and_get(self, scope: RequestScope):
        payment = make_payment(amount=Decimal("12.30"), metadata={"order": {"id": 7}})
        async with scope.uow:
            assert await scope.payment_commands.add(payment)

        async with scope.uow:
            loaded = present(await scope.payment_queries.get(payment.id))
            by_key = await scope.payment_queries.get_by_idempotency_key(payment.idempotency_key)

        assert loaded == by_key == payment
        assert loaded.amount == Decimal("12.30")
        assert loaded.metadata == {"order": {"id": 7}}
        assert loaded.status is PaymentStatus.PENDING
        assert loaded.created_at == payment.created_at
        assert loaded.created_at.tzinfo is not None
        assert loaded.version == 0

    async def test_get_missing(self, scope: RequestScope):
        async with scope.uow:
            assert await scope.payment_queries.get(make_payment().id) is None
            assert await scope.payment_queries.get_by_idempotency_key("missing") is None

    async def test_write_visible_to_read_in_same_transaction(self, scope: RequestScope):
        payment = make_payment()
        async with scope.uow:
            await scope.payment_commands.add(payment)
            assert await scope.payment_queries.get(payment.id) == payment

    async def test_add_duplicate_idempotency_key(
        self, scope: RequestScope, session_factory: async_sessionmaker[AsyncSession]
    ):
        async with scope.uow:
            assert await scope.payment_commands.add(make_payment(idempotency_key="dup"))
        async with scope.uow:
            assert not await scope.payment_commands.add(make_payment(idempotency_key="dup"))

        assert await count(session_factory, PaymentModel) == 1

    async def test_update_bumps_version(self, scope: RequestScope):
        payment = make_payment()
        async with scope.uow:
            await scope.payment_commands.add(payment)

        async with scope.uow:
            loaded = present(await scope.payment_queries.get(payment.id))
            loaded.mark_succeeded()
            await scope.payment_commands.update(loaded, "status", "processed_at")
        assert loaded.version == 1

        async with scope.uow:
            stored = present(await scope.payment_queries.get(payment.id))
        assert stored.status is PaymentStatus.SUCCEEDED
        assert stored.processed_at is not None
        assert stored.version == 1

    async def test_optimistic_lock(self, open_scope: ScopeFactory):
        """Два consumer'а прочитали одну версию; второй update должен упасть."""
        payment = make_payment()
        async with open_scope() as a, open_scope() as b:
            async with a.uow:
                await a.payment_commands.add(payment)

            async with a.uow:
                first = present(await a.payment_queries.get(payment.id))
            async with b.uow:
                second = present(await b.payment_queries.get(payment.id))

            first.mark_succeeded()
            async with a.uow:
                await a.payment_commands.update(first, "status", "processed_at")

            second.mark_failed()
            with pytest.raises(ConcurrentUpdateError):
                async with b.uow:
                    await b.payment_commands.update(second, "status", "processed_at")

            async with b.uow:
                stored = present(await b.payment_queries.get(payment.id))
        assert stored.status is PaymentStatus.SUCCEEDED
        assert stored.version == 1

    async def test_reread_after_conflict_in_same_transaction(self, open_scope: ScopeFactory):
        """Сценарий CompletePayment: поймать конфликт и перечитать без rollback.

        Сессия общая на запрос, и перечитывание должно видеть запись соседа. Сейчас это так,
        потому что репозитории не удерживают ORM-объекты (identity map — weak refs).
        """
        payment = make_payment()
        async with open_scope() as a, open_scope() as b:
            async with a.uow:
                await a.payment_commands.add(payment)

            async with b.uow:
                stale = present(await b.payment_queries.get(payment.id))

                async with a.uow:
                    first = present(await a.payment_queries.get(payment.id))
                    first.mark_succeeded()
                    await a.payment_commands.update(first, "status", "processed_at")

                stale.mark_failed()
                with pytest.raises(ConcurrentUpdateError):
                    await b.payment_commands.update(stale, "status", "processed_at")
                fresh = present(await b.payment_queries.get(payment.id))

        assert fresh.status is PaymentStatus.SUCCEEDED
        assert fresh.version == 1

    @pytest.mark.parametrize("fields", [(), ("amount",), ("status", "metadata")])
    async def test_update_rejects_fields(self, scope: RequestScope, fields: tuple[str, ...]):
        async with scope.uow:
            with pytest.raises(ValueError, match="non-empty subset"):
                await scope.payment_commands.update(make_payment(), *fields)


MAX_ATTEMPTS = 3


class TestOutboxRepositories:
    async def test_publish_cycle(self, scope: RequestScope):
        payment = make_payment()
        [event] = payment.pull_events()
        async with scope.uow:
            await scope.outbox_commands.add_events([event])

        async with scope.uow:
            [row] = await scope.outbox_queries.fetch_for_update(10, MAX_ATTEMPTS)
            assert row.id == event.event_id
            assert row.event_type == "payments.new"
            assert row.payload == {"payment_id": str(payment.id)}
            assert (row.attempts, row.last_error) == (0, None)
            await scope.outbox_commands.delete([row.id])

        async with scope.uow:
            assert await scope.outbox_queries.fetch_for_update(10, MAX_ATTEMPTS) == []

    async def test_empty_inputs_are_noop(self, scope: RequestScope):
        async with scope.uow:
            await scope.outbox_commands.add_events([])
            await scope.outbox_commands.delete([])
            await scope.outbox_commands.record_failures({})

    async def test_skip_locked(self, scope: RequestScope, open_scope: ScopeFactory):
        events = [e for _ in range(2) for e in make_payment().pull_events()]
        async with scope.uow:
            await scope.outbox_commands.add_events(events)

        async with open_scope() as a, open_scope() as b, a.uow, b.uow:
            [row_a] = await a.outbox_queries.fetch_for_update(1, MAX_ATTEMPTS)
            [row_b] = await b.outbox_queries.fetch_for_update(1, MAX_ATTEMPTS)

        assert {row_a.id, row_b.id} == {e.event_id for e in events}

    async def test_failures_are_counted_per_row(self, scope: RequestScope):
        poison, other = (e for _ in range(2) for e in make_payment().pull_events())
        async with scope.uow:
            await scope.outbox_commands.add_events([poison, other])

        async with scope.uow:
            await scope.outbox_commands.record_failures({poison.event_id: "no route"})
            await scope.outbox_commands.record_failures({poison.event_id: "no route again"})
            rows = await scope.outbox_queries.fetch_for_update(10, MAX_ATTEMPTS)

        by_id = {row.id: row for row in rows}
        assert (by_id[poison.event_id].attempts, by_id[poison.event_id].last_error) == (
            2,
            "no route again",
        )
        assert by_id[other.event_id].attempts == 0

    async def test_exhausted_rows_are_skipped_not_deleted(
        self, scope: RequestScope, session_factory: async_sessionmaker[AsyncSession]
    ):
        poison, fresh = (e for _ in range(2) for e in make_payment().pull_events())
        async with scope.uow:
            await scope.outbox_commands.add_events([poison])
        async with scope.uow:
            await scope.outbox_commands.add_events([fresh])  # created_at позже, чем у poison
        async with scope.uow:
            for _ in range(MAX_ATTEMPTS):
                await scope.outbox_commands.record_failures({poison.event_id: "no route"})

        async with scope.uow:
            # limit=1: poison старше, но больше не занимает место в пачке
            [row] = await scope.outbox_queries.fetch_for_update(1, MAX_ATTEMPTS)

        assert row.id == fresh.event_id
        assert await count(session_factory, OutboxModel) == 2


class TestUnitOfWork:
    async def test_rollback_on_error(
        self, scope: RequestScope, session_factory: async_sessionmaker[AsyncSession]
    ):
        payment = make_payment()
        # PT012: исключение намеренно внутри UoW — проверяем, что он откатывает всё
        with pytest.raises(RuntimeError, match="boom"):  # noqa: PT012
            async with scope.uow:
                await scope.payment_commands.add(payment)
                await scope.outbox_commands.add_events(payment.pull_events())
                raise RuntimeError("boom")

        assert await count(session_factory, PaymentModel) == 0
        assert await count(session_factory, OutboxModel) == 0

    async def test_commit_payment_and_event_together(
        self, scope: RequestScope, session_factory: async_sessionmaker[AsyncSession]
    ):
        payment = make_payment()
        async with scope.uow:
            await scope.payment_commands.add(payment)
            await scope.outbox_commands.add_events(payment.pull_events())

        assert await count(session_factory, PaymentModel) == 1
        assert await count(session_factory, OutboxModel) == 1

    async def test_session_reused_after_rollback(
        self, scope: RequestScope, session_factory: async_sessionmaker[AsyncSession]
    ):
        """Повторный `async with uow:` на той же сессии — новая транзакция."""
        with pytest.raises(RuntimeError, match="boom"):  # noqa: PT012 — исключение внутри UoW
            async with scope.uow:
                await scope.payment_commands.add(make_payment())
                raise RuntimeError("boom")

        async with scope.uow:
            await scope.payment_commands.add(make_payment())

        assert await count(session_factory, PaymentModel) == 1
