from decimal import Decimal
from uuid import UUID


class DomainError(Exception):
    """Базовая ошибка доменного слоя."""


class InvalidPaymentAmount(DomainError):
    def __init__(self, amount: Decimal) -> None:
        self.amount = amount
        super().__init__(f"Payment amount must be positive, got {amount}")


class InvalidStatusTransition(DomainError):
    def __init__(self, payment_id: UUID, current: str, target: str) -> None:
        self.payment_id = payment_id
        self.current = current
        self.target = target
        super().__init__(f"Payment {payment_id}: cannot transition from {current} to {target}")


class PaymentNotFound(DomainError):
    def __init__(self, payment_id: UUID) -> None:
        self.payment_id = payment_id
        super().__init__(f"Payment {payment_id} not found")


class IdempotencyKeyConflict(DomainError):
    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__(
            f"Idempotency key {idempotency_key!r} was already used with a different request body"
        )
