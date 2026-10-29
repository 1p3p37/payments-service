from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import TIMESTAMP, BigInteger, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.domain.payment import Currency, Payment, PaymentStatus


class Base(DeclarativeBase):
    type_annotation_map = {datetime: TIMESTAMP(timezone=True)}  # noqa: RUF012


class PaymentModel(Base):
    __tablename__ = "payments"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str]
    description: Mapped[str]
    # атрибут `metadata` зарезервирован Declarative
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    status: Mapped[str]
    idempotency_key: Mapped[str] = mapped_column(unique=True)
    request_hash: Mapped[str]
    webhook_url: Mapped[str]
    created_at: Mapped[datetime]
    processed_at: Mapped[datetime | None]
    version: Mapped[int] = mapped_column(BigInteger)

    def to_entity(self) -> Payment:
        return Payment(
            id=self.id,
            created_at=self.created_at,
            amount=self.amount,
            currency=Currency(self.currency),
            description=self.description,
            metadata=self.metadata_,
            idempotency_key=self.idempotency_key,
            request_hash=self.request_hash,
            webhook_url=self.webhook_url,
            status=PaymentStatus(self.status),
            processed_at=self.processed_at,
            version=self.version,
        )


class OutboxModel(Base):
    __tablename__ = "outbox"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    event_type: Mapped[str]
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime]
    # отказы брокера по этому сообщению; при OUTBOX_MAX_ATTEMPTS relay его пропускает
    attempts: Mapped[int]
    last_error: Mapped[str | None]
