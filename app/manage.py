"""CLI сервиса: `python -m app.manage <command>`."""

import asyncio
import contextlib
import logging
import signal

import click
from dishka import AsyncContainer

from app.application.commands.publish_outbox import PublishOutboxCommand
from app.application.mediator import Mediator
from app.infra.broker import create_broker, declare_topology
from app.infra.uow import UnitOfWork
from app.ioc import make_container
from app.settings import Settings, get_settings

logger = logging.getLogger("app.manage")


@click.group()
def cli() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


async def process_outbox(container: AsyncContainer) -> bool:
    """Одна пачка outbox в своём скоупе (сессии) и транзакции; возвращает has_more."""
    async with container() as request:
        uow = await request.get(UnitOfWork)
        mediator = await request.get(Mediator)
        async with uow:
            return await mediator.send(PublishOutboxCommand())


async def outbox_loop(container: AsyncContainer, settings: Settings, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            has_more = await process_outbox(container)
        except Exception:
            logger.exception("Outbox failed")
            has_more = False
        if not has_more:
            # пауза, но по SIGTERM просыпаемся сразу
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), settings.OUTBOX_POLL_INTERVAL)


@cli.command()
def runoutbox() -> None:
    """Outbox relay: переносит события из таблицы outbox в RabbitMQ.

    Можно запускать несколько реплик: строки берутся через FOR UPDATE SKIP LOCKED.
    """

    async def run() -> None:
        settings = get_settings()
        broker = create_broker(settings)
        container = make_container(settings, broker=broker)

        # Без обработчика SIGTERM процесс убивается сразу (а как PID 1 в контейнере
        # сигнал вообще игнорируется до SIGKILL), и finally не выполняется.
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)

        await broker.connect()
        try:
            await declare_topology(broker, settings)
            logger.info("Outbox relay started")
            await outbox_loop(container, settings, stop)  # текущая пачка доводится до конца
        finally:
            await asyncio.wait_for(container.close(), settings.OUTBOX_SHUTDOWN_TIMEOUT)
            await broker.stop()
            logger.info("Outbox relay stopped")

    asyncio.run(run())


if __name__ == "__main__":
    cli()
