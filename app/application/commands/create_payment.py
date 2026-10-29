from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from app.application.mediator import Command
from app.domain.exceptions import IdempotencyKeyConflict
from app.domain.payment import Currency, Payment, quantize_amount
from app.infra.repositories.commands.outbox import OutboxCommandRepository
from app.infra.repositories.commands.payment import PaymentCommandRepository
from app.infra.repositories.queries.payment import PaymentQueryRepository


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand(Command[Payment]):
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    webhook_url: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def request_hash(self) -> str:
        """Хэш тела запроса: тот же ключ с другим телом — конфликт, а не повтор."""
        return Payment.make_request_hash(
            {
                "amount": quantize_amount(self.amount),  # 10.0 и 10.00 — один и тот же запрос
                "currency": self.currency,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": self.webhook_url,
            }
        )


@dataclass
class CreatePaymentHandler:
    payment_queries: PaymentQueryRepository
    payment_commands: PaymentCommandRepository
    outbox_commands: OutboxCommandRepository

    async def handle(self, command: CreatePaymentCommand) -> Payment:
        request_hash = command.request_hash()
        if existing := await self._find_existing(command.idempotency_key, request_hash):
            return existing

        payment = Payment.create(
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            metadata=command.metadata,
            idempotency_key=command.idempotency_key,
            request_hash=request_hash,
            webhook_url=command.webhook_url,
        )
        if not await self.payment_commands.add(payment):
            # гонка двух одинаковых запросов: ON CONFLICT дождался коммита соседа,
            # его строка уже видна (READ COMMITTED)
            existing = await self._find_existing(command.idempotency_key, request_hash)
            if existing is None:  # конфликт по ключу, но строки нет — нарушен инвариант БД
                raise RuntimeError(
                    f"Payment with idempotency key {command.idempotency_key!r} "
                    "conflicted on insert but was not found"
                )
            return existing

        # в той же транзакции, что и платёж (outbox pattern)
        await self.outbox_commands.add_events(payment.pull_events())
        return payment

    async def _find_existing(self, idempotency_key: str, request_hash: str) -> Payment | None:
        existing = await self.payment_queries.get_by_idempotency_key(idempotency_key)
        if existing is not None and existing.request_hash != request_hash:
            raise IdempotencyKeyConflict(idempotency_key)
        return existing
