import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from typing import Any, ClassVar, Self
from uuid import UUID

from app.domain.base import BaseEntity, BaseEvent
from app.domain.exceptions import InvalidPaymentAmount, InvalidStatusTransition

AMOUNT_QUANT = Decimal("0.01")


def quantize_amount(amount: Decimal) -> Decimal:
    return amount.quantize(AMOUNT_QUANT, rounding=ROUND_HALF_UP)


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class PaymentCreatedEvent(BaseEvent):
    event_name: ClassVar[str] = "payments.new"  # совпадает с routing key
    payment_id: UUID


@dataclass(eq=False)
class Payment(BaseEntity):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    idempotency_key: str
    request_hash: str
    webhook_url: str
    status: PaymentStatus = PaymentStatus.PENDING
    processed_at: datetime | None = None
    version: int = 0

    @classmethod
    def create(
        cls,
        *,
        amount: Decimal,
        currency: Currency | str,
        description: str,
        metadata: dict[str, Any],
        idempotency_key: str,
        request_hash: str,
        webhook_url: str,
    ) -> Self:
        if amount <= 0:
            raise InvalidPaymentAmount(amount)
        payment = cls(
            amount=quantize_amount(amount),
            currency=Currency(currency),
            description=description,
            metadata=metadata,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=webhook_url,
        )
        payment.register_event(PaymentCreatedEvent(payment_id=payment.id))
        return payment

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "amount": self.amount,
            "currency": self.currency,
            "description": self.description,
            "metadata": self.metadata,
            "idempotency_key": self.idempotency_key,
            "request_hash": self.request_hash,
            "webhook_url": self.webhook_url,
            "status": self.status,
            "processed_at": self.processed_at,
            "version": self.version,
        }

    @property
    def is_pending(self) -> bool:
        return self.status is PaymentStatus.PENDING

    def mark_succeeded(self) -> None:
        self._finish(PaymentStatus.SUCCEEDED)

    def mark_failed(self) -> None:
        self._finish(PaymentStatus.FAILED)

    def _finish(self, target: PaymentStatus) -> None:
        if not self.is_pending:
            raise InvalidStatusTransition(self.id, self.status, target)
        self.status = target
        self.processed_at = datetime.now(UTC)

    @staticmethod
    def make_request_hash(data: Mapping[str, Any]) -> str:
        canonical = json.dumps(data, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.blake2b(canonical.encode()).hexdigest()
