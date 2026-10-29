import json
from collections.abc import Callable
from decimal import Decimal
from uuid import uuid4

import pytest

from app.domain.exceptions import InvalidPaymentAmount, InvalidStatusTransition
from app.domain.payment import (
    Currency,
    Payment,
    PaymentCreatedEvent,
    PaymentStatus,
    quantize_amount,
)
from tests.factories import make_payment


class TestCreate:
    def test_defaults(self):
        payment = make_payment()

        assert payment.status is PaymentStatus.PENDING
        assert payment.is_pending
        assert payment.version == 0
        assert payment.processed_at is None
        assert payment.created_at.tzinfo is not None

    def test_amount_is_quantized(self):
        assert make_payment(amount=Decimal("10.005")).amount == Decimal("10.01")
        assert make_payment(amount=Decimal("7")).amount == Decimal("7.00")


@pytest.mark.parametrize(
    ("amount", "expected"),
    [("10", "10.00"), ("10.0", "10.00"), ("10.005", "10.01"), ("10.004", "10.00")],
)
def test_quantize_amount(amount: str, expected: str):
    result = quantize_amount(Decimal(amount))

    assert str(result) == expected

    def test_currency_from_string(self):
        assert make_payment(currency="USD").currency is Currency.USD

    def test_unknown_currency(self):
        with pytest.raises(ValueError, match="not a valid Currency"):
            make_payment(currency="GBP")

    @pytest.mark.parametrize("amount", [Decimal("0"), Decimal("-1.00")])
    def test_non_positive_amount(self, amount: Decimal):
        with pytest.raises(InvalidPaymentAmount):
            make_payment(amount=amount)


class TestEvents:
    def test_created_event_registered(self):
        payment = make_payment()

        events = payment.pull_events()

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, PaymentCreatedEvent)
        assert event.event_name == "payments.new"
        assert event.payment_id == payment.id
        assert payment.pull_events() == []

    def test_to_dict_is_json_payload(self):
        payment = make_payment()
        [event] = payment.pull_events()

        data = event.to_dict()

        assert data == {"payment_id": str(payment.id)}
        json.dumps(data)


class TestStatusTransitions:
    @pytest.mark.parametrize(
        ("mark", "expected"),
        [
            (Payment.mark_succeeded, PaymentStatus.SUCCEEDED),
            (Payment.mark_failed, PaymentStatus.FAILED),
        ],
    )
    def test_from_pending(self, mark: Callable[[Payment], None], expected: PaymentStatus):
        payment = make_payment()

        mark(payment)

        assert payment.status is expected
        assert payment.processed_at is not None
        assert not payment.is_pending

    @pytest.mark.parametrize("first", [Payment.mark_succeeded, Payment.mark_failed])
    @pytest.mark.parametrize("second", [Payment.mark_succeeded, Payment.mark_failed])
    def test_only_from_pending(
        self, first: Callable[[Payment], None], second: Callable[[Payment], None]
    ):
        payment = make_payment()
        first(payment)

        with pytest.raises(InvalidStatusTransition):
            second(payment)


class TestRequestHash:
    def test_stable_regardless_of_key_order(self):
        a = {"amount": "10.00", "currency": "RUB", "metadata": {"x": 1, "y": 2}}
        b = {"metadata": {"y": 2, "x": 1}, "currency": "RUB", "amount": "10.00"}

        assert Payment.make_request_hash(a) == Payment.make_request_hash(b)

    def test_differs_on_value_change(self):
        a = {"amount": "10.00"}
        b = {"amount": "10.01"}

        assert Payment.make_request_hash(a) != Payment.make_request_hash(b)

    def test_non_json_types(self):
        data = {"amount": Decimal("10.00"), "id": uuid4()}

        assert Payment.make_request_hash(data) == Payment.make_request_hash(dict(data))


def test_entity_equality_by_id():
    payment = make_payment()
    other = make_payment()

    assert payment != other
    assert payment == Payment(**{**payment.__dict__, "_events": []})
    assert len({payment, payment}) == 1


def test_to_dict_has_all_fields():
    payment = make_payment()

    data = payment.to_dict()

    assert data["id"] == payment.id
    assert data["metadata"] == payment.metadata
    assert "_events" not in data
