"""Поведение очередей на настоящем RabbitMQ: TestRabbitBroker не эмулирует TTL и DLX."""

import json
import time

from faststream.rabbit import RabbitBroker

from app.infra.broker import (
    DEAD_LETTER_QUEUE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW,
    PAYMENTS_NEW_QUEUE,
    RETRY_EXCHANGE,
    declare_topology,
    retry_routing_key,
)
from app.settings import Settings
from tests.integration.rabbit import wait_message


async def test_declare_is_idempotent(broker: RabbitBroker, settings: Settings):
    # с теми же аргументами повторный declare не даёт PRECONDITION_FAILED
    await declare_topology(broker, settings)


async def test_retry_queue_returns_message_after_ttl(broker: RabbitBroker, settings: Settings):
    started = time.monotonic()
    await broker.publish(
        {"payment_id": "p1"},
        exchange=RETRY_EXCHANGE,
        routing_key=retry_routing_key(2),
        headers={"x-retry-count": 2},
        persist=True,
    )

    assert await wait_message(broker, PAYMENTS_NEW_QUEUE, timeout=0) is None  # ещё лежит в retry
    message = await wait_message(broker, PAYMENTS_NEW_QUEUE)

    assert message is not None
    assert json.loads(message.body) == {"payment_id": "p1"}
    assert message.headers["x-retry-count"] == 2
    # попытка 2 → 2·base
    assert time.monotonic() - started >= 2 * settings.RETRY_BASE_DELAY


async def test_rejected_message_goes_to_dlq(broker: RabbitBroker):
    await broker.publish(
        {"payment_id": "p1"}, exchange=PAYMENTS_EXCHANGE, routing_key=PAYMENTS_NEW, persist=True
    )
    message = await wait_message(broker, PAYMENTS_NEW_QUEUE, no_ack=False)
    assert message is not None

    await message.reject(requeue=False)

    dead = await wait_message(broker, DEAD_LETTER_QUEUE)
    assert dead is not None
    assert json.loads(dead.body) == {"payment_id": "p1"}
    assert await wait_message(broker, PAYMENTS_NEW_QUEUE, timeout=0) is None
