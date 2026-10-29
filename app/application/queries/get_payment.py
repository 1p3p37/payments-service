from dataclasses import dataclass
from uuid import UUID

from app.application.mediator import Query
from app.domain.exceptions import PaymentNotFound
from app.domain.payment import Payment
from app.infra.repositories.queries.payment import PaymentQueryRepository


@dataclass(frozen=True, slots=True)
class GetPaymentQuery(Query[Payment]):
    payment_id: UUID


@dataclass
class GetPaymentHandler:
    payment_queries: PaymentQueryRepository

    async def handle(self, query: GetPaymentQuery) -> Payment:
        payment = await self.payment_queries.get(query.payment_id)
        if payment is None:
            raise PaymentNotFound(query.payment_id)
        return payment
