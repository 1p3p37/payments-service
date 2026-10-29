import asyncio
import json
from decimal import Decimal
from uuid import uuid4

import pytest
import respx
from dishka import AsyncContainer
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.commands.charge_payment import ChargePaymentCommand
from app.application.commands.complete_payment import CompletePaymentCommand
from app.application.commands.create_payment import CreatePaymentCommand
from app.application.commands.send_webhook import SendWebhookCommand
from app.application.mediator import Mediator
from app.application.queries.get_payment import GetPaymentQuery
from app.domain.exceptions import IdempotencyKeyConflict, PaymentNotFound
from app.domain.payment import Currency, Payment, PaymentStatus
from app.infra.models import OutboxModel, PaymentModel
from app.infra.repositories.queries.payment import PaymentQueryRepository
from app.infra.uow import UnitOfWork

WEBHOOK_URL = "http://client.test/hook"


def create_command(**overrides) -> CreatePaymentCommand:
    params = {
        "idempotency_key": "key-1",
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": WEBHOOK_URL,
    } | overrides
    return CreatePaymentCommand(**params)


async def send(container: AsyncContainer, command):
    """Как точка входа: свой скоуп и транзакция на каждый вызов."""
    async with container() as request:
        uow = await request.get(UnitOfWork)
        mediator = await request.get(Mediator)
        async with uow:
            if isinstance(command, GetPaymentQuery):
                return await mediator.query(command)
            return await mediator.send(command)


async def counts(session_factory: async_sessionmaker[AsyncSession]) -> tuple[int, int]:
    async with session_factory() as session:
        payments = await session.scalar(select(func.count()).select_from(PaymentModel))
        outbox = await session.scalar(select(func.count()).select_from(OutboxModel))
    return payments, outbox


class TestCreatePayment:
    async def test_creates_payment_and_event(
        self, container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
    ):
        payment = await send(container, create_command())

        assert payment.status is PaymentStatus.PENDING
        assert await counts(session_factory) == (1, 1)

    @pytest.mark.parametrize("amount", ["100.00", "100.0", "100"])
    async def test_repeat_returns_same_payment(
        self,
        container: AsyncContainer,
        session_factory: async_sessionmaker[AsyncSession],
        amount: str,
    ):
        first = await send(container, create_command())
        repeat = await send(container, create_command(amount=Decimal(amount)))

        assert repeat.id == first.id
        assert await counts(session_factory) == (1, 1)

    async def test_same_key_different_body(self, container: AsyncContainer):
        await send(container, create_command())

        with pytest.raises(IdempotencyKeyConflict):
            await send(container, create_command(amount=Decimal("200.00")))

    async def test_concurrent_duplicates(
        self, container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
    ):
        results = await asyncio.gather(*(send(container, create_command()) for _ in range(5)))

        assert len({p.id for p in results}) == 1
        assert await counts(session_factory) == (1, 1)


class TestGetPayment:
    async def test_found(self, container: AsyncContainer):
        created = await send(container, create_command())

        assert await send(container, GetPaymentQuery(created.id)) == created

    async def test_not_found(self, container: AsyncContainer):
        with pytest.raises(PaymentNotFound):
            await send(container, GetPaymentQuery(uuid4()))


class TestCompletePayment:
    @pytest.mark.parametrize(
        ("succeeded", "status"), [(True, PaymentStatus.SUCCEEDED), (False, PaymentStatus.FAILED)]
    )
    async def test_writes_result(
        self, container: AsyncContainer, succeeded: bool, status: PaymentStatus
    ):
        created = await send(container, create_command())

        completed = await send(container, CompletePaymentCommand(created.id, succeeded))

        stored = await send(container, GetPaymentQuery(created.id))
        assert completed.status is stored.status is status
        assert stored.processed_at is not None
        assert stored.version == 1

    async def test_repeat_is_noop(self, container: AsyncContainer):
        created = await send(container, create_command())
        first = await send(container, CompletePaymentCommand(created.id, succeeded=True))

        # повторная доставка с другим исходом ничего не меняет
        repeat = await send(container, CompletePaymentCommand(created.id, succeeded=False))

        assert repeat.status is PaymentStatus.SUCCEEDED
        assert repeat.version == first.version == 1

    async def test_concurrent_writer_wins(self, container: AsyncContainer):
        """Сосед записал результат между нашим чтением и update."""
        created = await send(container, create_command())

        async with container() as request:
            uow = await request.get(UnitOfWork)
            mediator = await request.get(Mediator)
            # тот же экземпляр получит хендлер: репозитории кэшируются в пределах скоупа
            queries = await request.get(PaymentQueryRepository)
            original_get = queries.get

            async def get_then_neighbour_writes(payment_id):
                payment = await original_get(payment_id)
                if payment.is_pending:  # только первое чтение, перечитывание — без вмешательства
                    await send(container, CompletePaymentCommand(payment_id, succeeded=True))
                return payment

            queries.get = get_then_neighbour_writes
            async with uow:
                result = await mediator.send(CompletePaymentCommand(created.id, succeeded=False))

        assert result.status is PaymentStatus.SUCCEEDED
        assert result.version == 1

    async def test_not_found(self, container: AsyncContainer):
        with pytest.raises(PaymentNotFound):
            await send(container, CompletePaymentCommand(uuid4(), succeeded=True))


@respx.mock
async def test_consumer_flow_without_broker(container: AsyncContainer):
    """Шаги будущего consumer'а: Get → is_pending → Charge → Complete → SendWebhook."""
    route = respx.post(WEBHOOK_URL).respond(200)
    created = await send(container, create_command())

    async with container() as request:
        uow = await request.get(UnitOfWork)
        mediator = await request.get(Mediator)

        async with uow:
            payment: Payment = await mediator.query(GetPaymentQuery(created.id))
        if payment.is_pending:
            succeeded = await mediator.send(
                ChargePaymentCommand(payment.id, payment.amount, payment.currency)
            )
            async with uow:
                payment = await mediator.send(CompletePaymentCommand(payment.id, succeeded))
        await mediator.send(SendWebhookCommand(payment))

    assert route.call_count == 1
    body = json.loads(route.calls.last.request.content)
    assert body["payment_id"] == str(created.id)
    assert body["status"] == "succeeded"
    stored = await send(container, GetPaymentQuery(created.id))
    assert stored.status is PaymentStatus.SUCCEEDED
