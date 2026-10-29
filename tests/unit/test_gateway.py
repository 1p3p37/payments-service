from decimal import Decimal
from uuid import uuid4

import pytest

from app.domain.payment import Currency
from app.infra.gateway import PaymentGateway


def gateway(success_rate: float = 0.9) -> PaymentGateway:
    return PaymentGateway(success_rate=success_rate, min_delay=0, max_delay=0)


async def charge(gw: PaymentGateway, payment_id=None) -> bool:
    return await gw.charge(payment_id or uuid4(), Decimal("10.00"), Currency.RUB)


async def test_same_payment_same_result():
    gw = gateway()
    payment_id = uuid4()

    results = {await charge(gw, payment_id) for _ in range(5)}

    assert len(results) == 1


async def test_success_rate_is_respected():
    gw = gateway(0.9)

    results = [await charge(gw) for _ in range(2000)]

    assert 0.87 <= sum(results) / len(results) <= 0.93


@pytest.mark.parametrize(("rate", "expected"), [(0.0, False), (1.0, True)])
async def test_edge_rates(rate: float, expected: bool):
    gw = gateway(rate)

    assert {await charge(gw) for _ in range(50)} == {expected}
