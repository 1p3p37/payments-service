"""Топология RabbitMQ. Объявляется на старте и consumer'ом, и relay.

    relay ──► [payments] ──payments.new──► payments.new ──► consumer
                  ▲                            │ reject
                  │ TTL истёк                  ▼
    consumer ──► [payments.retry] ──retry.N──► payments.new.retry.N   (TTL = base·2^(N-1))
                                               [payments.dlx] ──► payments.new.dlq

Своя очередь на каждую попытку: TTL на уровне очереди, поэтому нет head-of-line
блокировки, как при per-message expiration в одной общей очереди.

Аргументы очередей должны совпадать при каждом declare, иначе RabbitMQ вернёт
PRECONDITION_FAILED: после смены MAX_RETRIES / RETRY_BASE_DELAY retry-очереди нужно удалить.
"""

from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.settings import Settings

PAYMENTS_NEW = "payments.new"  # routing key = event_name события

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    PAYMENTS_NEW,
    durable=True,
    routing_key=PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": f"{PAYMENTS_NEW}.dlq",
    },
)
DEAD_LETTER_QUEUE = RabbitQueue(
    f"{PAYMENTS_NEW}.dlq", durable=True, routing_key=f"{PAYMENTS_NEW}.dlq"
)


def retry_delay_ms(attempt: int, base_delay: float) -> int:
    """Экспоненциальная задержка: попытка 1 → base, 2 → 2·base, 3 → 4·base."""
    return int(base_delay * 2 ** (attempt - 1) * 1000)


def retry_routing_key(attempt: int) -> str:
    return f"retry.{attempt}"


def retry_queue(attempt: int, base_delay: float) -> RabbitQueue:
    # consumer'ов нет: сообщение лежит TTL и через DLX возвращается в payments.new
    return RabbitQueue(
        f"{PAYMENTS_NEW}.retry.{attempt}",
        durable=True,
        routing_key=retry_routing_key(attempt),
        arguments={
            "x-message-ttl": retry_delay_ms(attempt, base_delay),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
        },
    )


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.RABBIT_URL,
        # on_return_raises: публикация без маршрута (очередь не объявлена) — ошибка,
        # а не молчаливая потеря; relay тогда не удалит строку из outbox
        default_channel=Channel(prefetch_count=10, on_return_raises=True),
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    bindings = [
        (PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE),
        (DEAD_LETTER_EXCHANGE, DEAD_LETTER_QUEUE),
        *(
            (RETRY_EXCHANGE, retry_queue(n, settings.RETRY_BASE_DELAY))
            for n in range(1, settings.MAX_RETRIES + 1)
        ),
    ]
    for exchange in (PAYMENTS_EXCHANGE, RETRY_EXCHANGE, DEAD_LETTER_EXCHANGE):
        await broker.declare_exchange(exchange)
    for exchange, queue in bindings:
        declared = await broker.declare_queue(queue)
        await declared.bind(exchange.name, routing_key=queue.routing_key)
