from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.payment import Payment
from app.infra.exceptions import ConcurrentUpdateError
from app.infra.models import PaymentModel

payments_table = PaymentModel.__table__


class PaymentCommandRepository:
    UPDATABLE_FIELDS = frozenset({"status", "processed_at"})

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, payment: Payment) -> bool:
        """False — платёж с таким idempotency_key уже вставлен параллельным запросом."""
        # через Table: ключи to_dict() совпадают с именами колонок (metadata, а не metadata_)
        stmt = (
            pg_insert(payments_table)
            .values(payment.to_dict())
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(payments_table.c.id)
        )
        return (await self.session.execute(stmt)).first() is not None

    async def update(self, payment: Payment, *fields: str) -> None:
        """Optimistic lock: обновляет только если версия в БД совпадает с версией сущности."""
        if not fields or not self.UPDATABLE_FIELDS.issuperset(fields):
            raise ValueError(f"Fields must be a non-empty subset of {set(self.UPDATABLE_FIELDS)}")
        stmt = (
            update(PaymentModel)
            .where(PaymentModel.id == payment.id, PaymentModel.version == payment.version)
            .values(**{f: getattr(payment, f) for f in fields}, version=payment.version + 1)
            .returning(PaymentModel.id)
        )
        if (await self.session.execute(stmt)).first() is None:
            raise ConcurrentUpdateError(payment.id)
        payment.version += 1
