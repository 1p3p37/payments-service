"""Consumer на настоящих Postgres + RabbitMQ: полный путь outbox → очередь → обработка.

Webhook перехватывает respx, шлюз — счётчик вызовов без задержки.
"""

import json
import time
from collections.abc import Iterator
from uuid import uuid4

import httpx
import pytest
import respx
from dishka import AsyncContainer
from faststream.rabbit import RabbitBroker

from app.application.queries.get_payment import GetPaymentQuery
from app.consumer.retry import RETRY_COUNT_HEADER
from app.domain.payment import Payment, PaymentStatus
from app.infra.broker import DEAD_LETTER_QUEUE, PAYMENTS_EXCHANGE, PAYMENTS_NEW
from app.manage import process_outbox
from app.settings import Settings
from tests.integration.conftest import CountingGateway
from tests.integration.helpers import WEBHOOK_URL, create_command, send, wait_until
from tests.integration.rabbit import wait_message


@pytest.fixture
def webhook() -> Iterator[respx.Route]:
    with respx.mock(assert_all_called=False) as mock:
        yield mock.post(WEBHOOK_URL)


async def create_and_publish(container: AsyncContainer, relay_container: AsyncContainer) -> Payment:
    """POST → outbox → relay: событие в очереди, как в проде."""
    payment = await send(container, create_command())
    assert await process_outbox(relay_container)
    return payment


async def stored(container: AsyncContainer, payment: Payment) -> Payment:
    return await send(container, GetPaymentQuery(payment.id))


def bodies(route: respx.Route) -> list[dict]:
    return [json.loads(call.request.content) for call in route.calls]


async def test_success(
    consumer: CountingGateway,
    container: AsyncContainer,
    relay_container: AsyncContainer,
    broker: RabbitBroker,
    webhook: respx.Route,
):
    webhook.respond(200)

    payment = await create_and_publish(container, relay_container)
    await wait_until(lambda: webhook.call_count == 1)

    result = await stored(container, payment)
    assert result.status is PaymentStatus.SUCCEEDED
    assert result.processed_at is not None
    assert bodies(webhook)[0]["status"] == "succeeded"
    assert webhook.calls.last.request.headers["X-Event-Id"] == str(payment.id)
    assert consumer.calls == [payment.id]
    assert await wait_message(broker, DEAD_LETTER_QUEUE, timeout=0) is None


async def test_webhook_fails_once(
    consumer: CountingGateway,
    container: AsyncContainer,
    relay_container: AsyncContainer,
    broker: RabbitBroker,
    webhook: respx.Route,
):
    webhook.side_effect = [httpx.Response(500), httpx.Response(200)]

    payment = await create_and_publish(container, relay_container)
    await wait_until(lambda: webhook.call_count == 2)

    # повтор увидел не-pending: шлюз второй раз не вызывался
    assert consumer.calls == [payment.id]
    assert (await stored(container, payment)).status is PaymentStatus.SUCCEEDED
    assert await wait_message(broker, DEAD_LETTER_QUEUE, timeout=0.5) is None


async def test_retries_exhausted_go_to_dlq(
    consumer: CountingGateway,
    container: AsyncContainer,
    relay_container: AsyncContainer,
    broker: RabbitBroker,
    rabbit_settings: Settings,
    webhook: respx.Route,
):
    webhook.respond(500)
    started = time.monotonic()

    payment = await create_and_publish(container, relay_container)
    dead = await wait_message(broker, DEAD_LETTER_QUEUE, timeout=10)

    assert dead is not None
    assert json.loads(dead.body) == {"payment_id": str(payment.id)}
    assert dead.headers[RETRY_COUNT_HEADER] == rabbit_settings.MAX_RETRIES
    # исходная попытка + MAX_RETRIES повторов с задержками base · (1 + 2 + 4)
    assert webhook.call_count == 1 + rabbit_settings.MAX_RETRIES
    assert time.monotonic() - started >= rabbit_settings.RETRY_BASE_DELAY * 7
    # результат платежа сохранён, шлюз вызван один раз
    assert (await stored(container, payment)).status is PaymentStatus.SUCCEEDED
    assert consumer.calls == [payment.id]


async def test_unknown_payment_goes_to_dlq_without_retries(
    consumer: CountingGateway, broker: RabbitBroker, webhook: respx.Route
):
    payment_id = str(uuid4())
    await broker.publish(
        {"payment_id": payment_id}, exchange=PAYMENTS_EXCHANGE, routing_key=PAYMENTS_NEW
    )

    dead = await wait_message(broker, DEAD_LETTER_QUEUE)

    assert dead is not None
    assert json.loads(dead.body) == {"payment_id": payment_id}
    assert RETRY_COUNT_HEADER not in (dead.headers or {})
    assert webhook.call_count == 0
    assert consumer.calls == []


async def test_malformed_message_goes_to_dlq(consumer: CountingGateway, broker: RabbitBroker):
    await broker.publish({"foo": 1}, exchange=PAYMENTS_EXCHANGE, routing_key=PAYMENTS_NEW)

    dead = await wait_message(broker, DEAD_LETTER_QUEUE)

    assert dead is not None
    assert RETRY_COUNT_HEADER not in (dead.headers or {})


async def test_redelivery_does_not_charge_twice(
    consumer: CountingGateway,
    container: AsyncContainer,
    relay_container: AsyncContainer,
    broker: RabbitBroker,
    webhook: respx.Route,
):
    webhook.respond(200)
    payment = await create_and_publish(container, relay_container)
    await wait_until(lambda: webhook.call_count == 1)

    # то же сообщение ещё раз — как после падения relay между publish и commit
    await broker.publish(
        {"payment_id": str(payment.id)}, exchange=PAYMENTS_EXCHANGE, routing_key=PAYMENTS_NEW
    )
    await wait_until(lambda: webhook.call_count == 2)

    assert consumer.calls == [payment.id]
    assert (await stored(container, payment)).version == 1
    assert [b["status"] for b in bodies(webhook)] == ["succeeded", "succeeded"]
