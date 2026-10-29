from decimal import Decimal
from typing import Any
from uuid import uuid4

from app.domain.payment import Currency, Payment


def make_payment(**overrides: Any) -> Payment:
    params: dict[str, Any] = {
        "amount": Decimal("100.50"),
        "currency": Currency.RUB,
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "idempotency_key": f"key-{uuid4()}",
        "request_hash": "hash",
        "webhook_url": "http://example.com/hook",
    } | overrides
    return Payment.create(**params)
