import json
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from app.domain.payment import Payment
from app.infra.exceptions import WebhookDeliveryError
from app.infra.webhook import WebhookSender
from tests.factories import make_payment
from tests.helpers import present

URL = "http://client.test/hook"


@pytest.fixture
async def sender() -> AsyncIterator[WebhookSender]:
    async with httpx.AsyncClient() as client:
        yield WebhookSender(client)


@pytest.fixture
def payment() -> Payment:
    payment = make_payment(webhook_url=URL, metadata={"order": 1})
    payment.mark_succeeded()
    return payment


@respx.mock
async def test_sends_payload(sender: WebhookSender, payment: Payment):
    route = respx.post(URL).respond(200)

    await sender.send(payment)

    request = route.calls.last.request
    assert request.headers["X-Event-Id"] == str(payment.id)
    assert json.loads(request.content) == {
        "payment_id": str(payment.id),
        "status": "succeeded",
        "amount": "100.50",
        "currency": "RUB",
        "processed_at": present(payment.processed_at).isoformat(),
        "metadata": {"order": 1},
    }


@respx.mock
async def test_non_2xx_is_error(sender: WebhookSender, payment: Payment):
    respx.post(URL).respond(500)

    with pytest.raises(WebhookDeliveryError, match="HTTP 500"):
        await sender.send(payment)


@respx.mock
async def test_network_error(sender: WebhookSender, payment: Payment):
    respx.post(URL).mock(side_effect=httpx.ConnectError("refused"))

    with pytest.raises(WebhookDeliveryError):
        await sender.send(payment)
