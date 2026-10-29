"""Политика повторов consumer'а: retry-очереди с экспоненциальной задержкой, затем DLQ.

Реализована как FastStream middleware, а не декоратор: хендлер не меняет сигнатуру
(FastStream и dishka разбирают её параметры), а AcknowledgementMiddleware стоит снаружи
пользовательских middleware — `RejectMessage` отсюда превращается в reject → DLX → DLQ.
"""

import logging
from typing import Any

from faststream import BaseMiddleware
from faststream.exceptions import NackMessage, RejectMessage
from faststream.message import StreamMessage
from faststream.rabbit import RabbitBroker

from app.infra.broker import RETRY_EXCHANGE, retry_routing_key

logger = logging.getLogger(__name__)

RETRY_COUNT_HEADER = "x-retry-count"


def decide(attempt: int, max_retries: int) -> int | None:
    """Номер следующей попытки или None — повторы исчерпаны, сообщение уходит в DLQ.

    `attempt` — сколько повторов уже было (0 у исходной доставки).
    """
    return attempt + 1 if attempt < max_retries else None


class RetryMiddleware(BaseMiddleware):
    """Подключается через `functools.partial(RetryMiddleware, broker=..., ...)`."""

    def __init__(
        self,
        msg: Any,
        /,
        *,
        broker: RabbitBroker,
        max_retries: int,
        non_retryable: tuple[type[Exception], ...],
        **kwargs: Any,
    ) -> None:
        super().__init__(msg, **kwargs)
        self.broker = broker
        self.max_retries = max_retries
        self.non_retryable = non_retryable

    async def consume_scope(self, call_next: Any, msg: StreamMessage[Any]) -> Any:
        try:
            return await call_next(msg)
        except self.non_retryable:
            logger.exception("Non-retryable error, message %s → DLQ", msg.message_id)
            raise RejectMessage() from None
        except Exception:
            attempt = int(msg.headers.get(RETRY_COUNT_HEADER, 0))
            next_attempt = decide(attempt, self.max_retries)
            if next_attempt is None:
                logger.exception(
                    "Retries exhausted (%d), message %s → DLQ", attempt, msg.message_id
                )
                raise RejectMessage() from None

            logger.warning(
                "Processing failed, message %s → retry %d/%d",
                msg.message_id,
                next_attempt,
                self.max_retries,
                exc_info=True,
            )
            await self._schedule_retry(msg, next_attempt)
            return None  # ack оригинала: копия вернётся в payments.new через TTL

    async def _schedule_retry(self, msg: StreamMessage[Any], attempt: int) -> None:
        try:
            await self.broker.publish(
                msg.body,
                exchange=RETRY_EXCHANGE,
                routing_key=retry_routing_key(attempt),
                headers={**msg.headers, RETRY_COUNT_HEADER: attempt},
                message_id=msg.message_id,
                content_type=msg.content_type,
                persist=True,
            )
        except Exception:
            # не смогли отложить — вернуть в очередь, а не отправлять в DLQ досрочно
            logger.exception("Failed to schedule retry for %s, requeue", msg.message_id)
            raise NackMessage(requeue=True) from None
