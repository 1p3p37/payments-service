import asyncio
import logging
from dataclasses import dataclass
from uuid import UUID

from aiormq.exceptions import DeliveryError
from faststream.rabbit import RabbitBroker

from app.application.mediator import Command
from app.infra.broker import PAYMENTS_EXCHANGE
from app.infra.models import OutboxModel
from app.infra.repositories.commands.outbox import OutboxCommandRepository
from app.infra.repositories.queries.outbox import OutboxQueryRepository
from app.settings import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PublishOutboxCommand(Command[bool]):
    """Публикует пачку событий из outbox; результат — `has_more` для цикла relay."""


@dataclass
class PublishOutboxHandler:
    """At-least-once: строка удаляется только после publisher confirm от RabbitMQ.

    Упасть между publish и commit — событие уйдёт повторно, consumer идемпотентен.

    Ошибки делятся на два вида:
    - DeliveryError — брокер отказал именно этому сообщению (нет маршрута, nack):
      attempts += 1, после OUTBOX_MAX_ATTEMPTS сообщение больше не выбирается;
    - всё остальное (брокер недоступен, сеть) — временный сбой, attempts не трогаем,
      иначе за время простоя брокера в карантин ушёл бы весь outbox.
    """

    outbox_queries: OutboxQueryRepository
    outbox_commands: OutboxCommandRepository
    broker: RabbitBroker
    settings: Settings

    async def handle(self, command: PublishOutboxCommand) -> bool:
        rows = await self.outbox_queries.fetch_for_update(
            self.settings.OUTBOX_BATCH_SIZE, self.settings.OUTBOX_MAX_ATTEMPTS
        )
        semaphore = asyncio.Semaphore(self.settings.OUTBOX_PUBLISH_CONCURRENCY)
        published: list[UUID] = []
        rejected: dict[UUID, str] = {}

        async def publish(row: OutboxModel) -> None:
            async with semaphore:
                try:
                    await self.broker.publish(
                        row.payload,
                        exchange=PAYMENTS_EXCHANGE,
                        routing_key=row.event_type,
                        message_id=str(row.id),  # = event_id: один id в outbox, очереди, логах
                        persist=True,
                    )
                except DeliveryError as exc:
                    logger.error("Broker rejected outbox message %s: %r", row.id, exc)
                    rejected[row.id] = repr(exc)
                except Exception:
                    logger.exception("Outbox publish failed for %s", row.id)
                else:
                    published.append(row.id)

        await asyncio.gather(*map(publish, rows))
        await self.outbox_commands.delete(published)
        await self.outbox_commands.record_failures(rejected)
        # продолжаем без паузы, только если продвинулись; иначе при лежащем брокере
        # получился бы горячий цикл SELECT → ошибка → SELECT
        return bool(published)
