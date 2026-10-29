from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.payment import Payment
from app.infra.models import PaymentModel


class PaymentQueryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._one(select(PaymentModel).where(PaymentModel.id == payment_id))

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return await self._one(select(PaymentModel).where(PaymentModel.idempotency_key == key))

    # SQLAlchemy 2.1: Select параметризуется типами колонок (не tuple, как в 2.0)
    async def _one(self, stmt: Select[PaymentModel]) -> Payment | None:
        model = (await self.session.execute(stmt)).scalar_one_or_none()
        return model.to_entity() if model else None
