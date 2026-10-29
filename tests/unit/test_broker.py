import pytest

from app.infra.broker import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW,
    retry_delay_ms,
    retry_queue,
    retry_routing_key,
)


@pytest.mark.parametrize(("attempt", "expected"), [(1, 1000), (2, 2000), (3, 4000)])
def test_exponential_delay(attempt: int, expected: int):
    assert retry_delay_ms(attempt, base_delay=1) == expected


def test_retry_queue_dead_letters_back_to_main_queue():
    queue = retry_queue(2, base_delay=0.5)

    assert queue.name == "payments.new.retry.2"
    assert queue.routing_key == retry_routing_key(2) == "retry.2"
    # FastStream дописывает x-queue-type, проверяем только свои аргументы
    assert (
        queue.arguments.items()
        >= {
            "x-message-ttl": 1000,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
        }.items()
    )
