import asyncio
import inspect
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

from dishka import AsyncContainer

from app.application.commands.create_payment import CreatePaymentCommand
from app.application.mediator import Command, Mediator, Query
from app.domain.payment import Currency
from app.infra.uow import UnitOfWork

WEBHOOK_URL = "http://client.test/hook"


def create_command(**overrides: Any) -> CreatePaymentCommand:
    params = {
        "idempotency_key": "key-1",
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": WEBHOOK_URL,
    } | overrides
    return CreatePaymentCommand(**params)


async def send[R](container: AsyncContainer, request: Command[R] | Query[R]) -> R:
    """Как точка входа: свой скоуп и транзакция на каждый вызов."""
    async with container() as scope:
        uow = await scope.get(UnitOfWork)
        mediator = await scope.get(Mediator)
        async with uow:
            if isinstance(request, Query):
                return await mediator.query(request)
            return await mediator.send(request)


async def wait_until[T](
    probe: Callable[[], T | Awaitable[T]], timeout: float = 10.0, interval: float = 0.05
) -> T:
    """Опрашивать `probe` (обычную или async), пока результат не станет truthy; вернуть его."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = probe()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return result
        if loop.time() >= deadline:
            raise TimeoutError(f"Condition not met within {timeout}s (last: {result!r})")
        await asyncio.sleep(interval)
