import asyncio
import hashlib
import random
from decimal import Decimal
from uuid import UUID

from app.domain.payment import Currency

_BUCKETS = 10_000


class PaymentGateway:
    """Эмуляция внешнего платёжного шлюза.

    Как у реального шлюза с ключом идемпотентности: повторный charge с тем же
    `payment_id` возвращает тот же результат. Исход детерминирован хэшем id, поэтому
    любой consumer на любой повторной доставке получит одинаковый ответ.
    """

    def __init__(self, success_rate: float, min_delay: float, max_delay: float) -> None:
        self.success_rate = success_rate
        self.min_delay = min_delay
        self.max_delay = max_delay

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> bool:
        # эмуляция задержки, не криптография
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))  # noqa: S311
        digest = hashlib.blake2b(payment_id.bytes, digest_size=8).digest()
        return int.from_bytes(digest) % _BUCKETS < self.success_rate * _BUCKETS
