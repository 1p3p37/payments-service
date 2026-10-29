import asyncio
import inspect
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any, overload

from dishka import AsyncContainer
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.commands.create_payment import CreatePaymentCommand
from app.application.mediator import Command, Mediator, Query
from app.domain.payment import Currency
from app.infra.models import OutboxModel, PaymentModel
from app.infra.uow import UnitOfWork

WEBHOOK_URL = "http://client.test/hook"


async def counts(session_factory: async_sessionmaker[AsyncSession]) -> tuple[int, int]:
    """Число строк в payments и outbox."""
    async with session_factory() as session:
        payments = (
            await session.execute(select(func.count()).select_from(PaymentModel))
        ).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxModel))).scalar_one()
    return payments, outbox


def create_command(**overrides: Any) -> CreatePaymentCommand:
    params: dict[str, Any] = {
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


@overload
async def wait_until[T](
    probe: Callable[[], Awaitable[T | None]], timeout: float = ..., interval: float = ...
) -> T: ...
@overload
async def wait_until[T](
    probe: Callable[[], T | None], timeout: float = ..., interval: float = ...
) -> T: ...
async def wait_until(
    probe: Callable[[], Any], timeout: float = 10.0, interval: float = 0.05
) -> Any:
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
