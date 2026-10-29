from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from dishka import AsyncContainer, Provider, Scope, provide
from faststream import TestApp
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.consumer.main import create_app as create_consumer
from app.domain.payment import Currency
from app.infra.broker import (
    DEAD_LETTER_QUEUE,
    PAYMENTS_NEW_QUEUE,
    create_broker,
    declare_topology,
    retry_queue,
)
from app.infra.db import create_engine, create_session_factory
from app.infra.gateway import PaymentGateway
from app.infra.repositories.commands.outbox import OutboxCommandRepository
from app.infra.repositories.commands.payment import PaymentCommandRepository
from app.infra.repositories.queries.outbox import OutboxQueryRepository
from app.infra.repositories.queries.payment import PaymentQueryRepository
from app.infra.uow import UnitOfWork
from app.ioc import make_container
from app.settings import Settings

ROOT = Path(__file__).resolve().parents[2]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if Path(item.fspath).is_relative_to(Path(__file__).parent):
            item.add_marker(pytest.mark.integration)


# ── Postgres ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def db_url() -> Iterator[str]:
    """Postgres в testcontainers с применёнными миграциями."""
    try:
        from testcontainers.community.postgres import PostgresContainer

        container = PostgresContainer("postgres:16", driver="asyncpg").start()
    except Exception as exc:  # Docker недоступен
        pytest.skip(f"Docker is not available: {exc}")

    try:
        url = container.get_connection_url()
        config = Config(ROOT / "alembic.ini")
        config.set_main_option("script_location", str(ROOT / "migrations"))
        config.set_main_option("sqlalchemy.url", url)
        config.attributes["configure_logger"] = False
        command.upgrade(config, "head")
        yield url
    finally:
        container.stop()


@pytest.fixture
async def engine(db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(db_url)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@dataclass
class RequestScope:
    """То, что DI-контейнер собирает на один запрос/сообщение: одна сессия на всех."""

    session: AsyncSession
    uow: UnitOfWork
    payment_queries: PaymentQueryRepository
    payment_commands: PaymentCommandRepository
    outbox_queries: OutboxQueryRepository
    outbox_commands: OutboxCommandRepository


ScopeFactory = Callable[[], AbstractAsyncContextManager[RequestScope]]


@pytest.fixture
def open_scope(session_factory: async_sessionmaker[AsyncSession]) -> ScopeFactory:
    @asynccontextmanager
    async def factory() -> AsyncIterator[RequestScope]:
        async with session_factory() as session:
            yield RequestScope(
                session=session,
                uow=UnitOfWork(session),
                payment_queries=PaymentQueryRepository(session),
                payment_commands=PaymentCommandRepository(session),
                outbox_queries=OutboxQueryRepository(session),
                outbox_commands=OutboxCommandRepository(session),
            )

    return factory


@pytest.fixture
async def scope(open_scope: ScopeFactory) -> AsyncIterator[RequestScope]:
    async with open_scope() as scope:
        yield scope


# ── RabbitMQ ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def rabbit_url() -> Iterator[str]:
    try:
        from testcontainers.community.rabbitmq import RabbitMqContainer

        container = RabbitMqContainer("rabbitmq:3.13-management").start()
    except Exception as exc:
        pytest.skip(f"Docker is not available: {exc}")

    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(container.port)
        yield f"amqp://{container.username}:{container.password}@{host}:{port}/"
    finally:
        container.stop()


@pytest.fixture
def settings(db_url: str) -> Settings:
    # одинаковые значения на всю сессию: аргументы очередей должны совпадать при declare
    return Settings(
        DB_URL=db_url,
        RETRY_BASE_DELAY=0.2,
        OUTBOX_BATCH_SIZE=10,
        OUTBOX_MAX_ATTEMPTS=2,
        OUTBOX_POLL_INTERVAL=0.05,
    )


@pytest.fixture
def rabbit_settings(settings: Settings, rabbit_url: str) -> Settings:
    return settings.model_copy(update={"RABBIT_URL": rabbit_url})


@pytest.fixture
async def broker(rabbit_settings: Settings) -> AsyncIterator[RabbitBroker]:
    """Брокер для публикации и проверки очередей; топология объявлена, очереди пусты."""
    broker = create_broker(rabbit_settings)
    await broker.connect()
    await declare_topology(broker, rabbit_settings)
    queues = [PAYMENTS_NEW_QUEUE, DEAD_LETTER_QUEUE]
    queues += [
        retry_queue(n, rabbit_settings.RETRY_BASE_DELAY)
        for n in range(1, rabbit_settings.MAX_RETRIES + 1)
    ]
    for queue in queues:
        await (await broker.declare_queue(queue)).purge()
    yield broker
    await broker.stop()


# ── DI-контейнер ─────────────────────────────────────────────────────────────


class InstantGatewayProvider(Provider):
    """Шлюз без задержки и всегда успешный — тесты не ждут 2–5 с и не зависят от хэша id."""

    @provide(scope=Scope.APP, override=True)
    def gateway(self) -> PaymentGateway:
        return PaymentGateway(success_rate=1.0, min_delay=0, max_delay=0)


@pytest.fixture
async def container(settings: Settings, engine: AsyncEngine) -> AsyncIterator[AsyncContainer]:
    # engine в зависимостях — ради TRUNCATE перед тестом
    container = make_container(settings, InstantGatewayProvider())
    yield container
    await container.close()


@pytest.fixture
async def relay_container(
    settings: Settings, engine: AsyncEngine, broker: RabbitBroker
) -> AsyncIterator[AsyncContainer]:
    container = make_container(settings, InstantGatewayProvider(), broker=broker)
    yield container
    await container.close()


class CountingGateway(PaymentGateway):
    """Мгновенный успешный шлюз, считающий вызовы — проверка «шлюз не вызван повторно»."""

    def __init__(self) -> None:
        super().__init__(success_rate=1.0, min_delay=0, max_delay=0)
        self.calls: list[UUID] = []

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> bool:
        self.calls.append(payment_id)
        return await super().charge(payment_id, amount, currency)


class GatewayProvider(Provider):
    def __init__(self, gateway: PaymentGateway) -> None:
        super().__init__()
        self._gateway = gateway

    @provide(scope=Scope.APP, override=True)
    def gateway(self) -> PaymentGateway:
        return self._gateway


@pytest.fixture
async def consumer(
    rabbit_settings: Settings, engine: AsyncEngine, broker: RabbitBroker
) -> AsyncIterator[CountingGateway]:
    """Запущенный consumer на настоящем RabbitMQ; `broker` — чтобы очереди были чистыми."""
    gateway = CountingGateway()
    app = create_consumer(rabbit_settings, GatewayProvider(gateway))
    async with TestApp(app):
        yield gateway
