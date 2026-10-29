from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from app.application.mediator import Command
from app.domain.payment import Currency
from app.infra.gateway import PaymentGateway


@dataclass(frozen=True, slots=True)
class ChargePaymentCommand(Command[bool]):
    """Вызов шлюза. БД не трогает: вызывается вне транзакции, т.к. шлюз отвечает 2–5 с."""

    payment_id: UUID
    amount: Decimal
    currency: Currency


@dataclass
class ChargePaymentHandler:
    gateway: PaymentGateway

    async def handle(self, command: ChargePaymentCommand) -> bool:
        return await self.gateway.charge(command.payment_id, command.amount, command.currency)
