import asyncio
import json
from dataclasses import dataclass
from typing import ClassVar

from aio_pika import DeliveryMode
from dishka import AsyncContainer
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.domain.base import BaseEvent
from app.infra.broker import PAYMENTS_NEW_QUEUE
from app.infra.models import OutboxModel
from app.ioc import make_container
from app.manage import outbox_loop, process_outbox
from app.settings import Settings
from tests.factories import make_payment
from tests.integration.conftest import InstantGatewayProvider, RequestScope
from tests.integration.rabbit import wait_message


@dataclass
class UnroutableEvent(BaseEvent):
    event_name: ClassVar[str] = "nowhere"  # нет очереди с таким routing key


class UnreachableBroker:
    async def publish(self, *args, **kwargs):
        raise ConnectionError("broker is down")


async def outbox_rows(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxModel]:
    async with session_factory() as session:
        return list((await session.execute(select(OutboxModel))).scalars())


async def add_events(scope: RequestScope, *events: BaseEvent) -> None:
    async with scope.uow:
        await scope.outbox_commands.add_events(list(events))


async def test_publishes_persistent_message_and_deletes_row(
    relay_container: AsyncContainer,
    broker: RabbitBroker,
    scope: RequestScope,
    session_factory: async_sessionmaker[AsyncSession],
):
    payment = make_payment()
    [event] = payment.pull_events()
    await add_events(scope, event)

    assert await process_outbox(relay_container) is True

    message = await wait_message(broker, PAYMENTS_NEW_QUEUE)
    assert message is not None
    assert json.loads(message.body) == {"payment_id": str(payment.id)}
    assert message.message_id == str(event.event_id)
    assert message.delivery_mode == DeliveryMode.PERSISTENT  # переживёт рестарт RabbitMQ
    assert await outbox_rows(session_factory) == []
    assert await process_outbox(relay_container) is False  # пусто → пауза


async def test_unroutable_message_is_counted_then_skipped(
    relay_container: AsyncContainer,
    broker: RabbitBroker,
    scope: RequestScope,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
):
    poison = UnroutableEvent()
    await add_events(scope, poison)
    [good] = make_payment().pull_events()
    await add_events(scope, good)  # позже poison

    # 1-я пачка: good опубликован, poison отклонён брокером
    assert await process_outbox(relay_container) is True
    [row] = await outbox_rows(session_factory)
    assert row.id == poison.event_id
    assert row.attempts == 1
    assert "PublishError" in row.last_error

    for _ in range(settings.OUTBOX_MAX_ATTEMPTS):
        await process_outbox(relay_container)

    # карантин: строка осталась для разбора, но больше не выбирается
    [row] = await outbox_rows(session_factory)
    assert row.attempts == settings.OUTBOX_MAX_ATTEMPTS
    assert await wait_message(broker, PAYMENTS_NEW_QUEUE) is not None  # good
    assert await wait_message(broker, PAYMENTS_NEW_QUEUE, timeout=0) is None


async def test_broker_down_does_not_count_attempts(
    settings: Settings,
    engine: AsyncEngine,
    scope: RequestScope,
    session_factory: async_sessionmaker[AsyncSession],
):
    [event] = make_payment().pull_events()
    await add_events(scope, event)
    container = make_container(settings, InstantGatewayProvider(), broker=UnreachableBroker())
    try:
        for _ in range(settings.OUTBOX_MAX_ATTEMPTS + 1):
            assert await process_outbox(container) is False  # не продвинулись → пауза
    finally:
        await container.close()

    [row] = await outbox_rows(session_factory)
    assert (row.attempts, row.last_error) == (0, None)


async def test_loop_stops_on_signal(relay_container: AsyncContainer, settings: Settings):
    stop = asyncio.Event()
    task = asyncio.create_task(outbox_loop(relay_container, settings, stop))
    await asyncio.sleep(0.2)

    stop.set()

    await asyncio.wait_for(task, timeout=1)
