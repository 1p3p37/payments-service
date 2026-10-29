from dataclasses import dataclass
from uuid import UUID

from app.application.mediator import Command
from app.domain.exceptions import PaymentNotFound
from app.domain.payment import Payment
from app.infra.exceptions import ConcurrentUpdateError
from app.infra.repositories.commands.payment import PaymentCommandRepository
from app.infra.repositories.queries.payment import PaymentQueryRepository


@dataclass(frozen=True, slots=True)
class CompletePaymentCommand(Command[Payment]):
    payment_id: UUID
    succeeded: bool


@dataclass
class CompletePaymentHandler:
    """Записывает результат шлюза. Безопасна при повторной и параллельной доставке."""

    payment_queries: PaymentQueryRepository
    payment_commands: PaymentCommandRepository

    async def handle(self, command: CompletePaymentCommand) -> Payment:
        payment = await self._load(command.payment_id)
        if not payment.is_pending:
            return payment  # уже записано предыдущей доставкой

        if command.succeeded:
            payment.mark_succeeded()
        else:
            payment.mark_failed()
        try:
            await self.payment_commands.update(payment, "status", "processed_at")
        except ConcurrentUpdateError:
            # параллельный consumer записал результат раньше — его и возвращаем
            return await self._load(command.payment_id)
        return payment

    async def _load(self, payment_id: UUID) -> Payment:
        payment = await self.payment_queries.get(payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id)
        return payment
