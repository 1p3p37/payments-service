from collections.abc import AsyncIterator

import httpx
from dishka import (
    AsyncContainer,
    Provider,
    Scope,
    from_context,
    make_async_container,
    provide,
    provide_all,
)
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.application.commands.charge_payment import ChargePaymentHandler
from app.application.commands.complete_payment import CompletePaymentHandler
from app.application.commands.create_payment import CreatePaymentHandler
from app.application.commands.publish_outbox import PublishOutboxHandler
from app.application.commands.send_webhook import SendWebhookHandler
from app.application.mediator import Mediator
from app.application.queries.get_payment import GetPaymentHandler
from app.application.registry import HANDLERS
from app.infra.db import create_engine, create_session_factory
from app.infra.gateway import PaymentGateway
from app.infra.repositories.commands.outbox import OutboxCommandRepository
from app.infra.repositories.commands.payment import PaymentCommandRepository
from app.infra.repositories.queries.outbox import OutboxQueryRepository
from app.infra.repositories.queries.payment import PaymentQueryRepository
from app.infra.uow import UnitOfWork
from app.infra.webhook import WebhookSender
from app.settings import Settings


class InfraProvider(Provider):
    settings = from_context(provides=Settings, scope=Scope.APP)
    # брокер создаёт точка входа: consumer'у он нужен на этапе импорта для @subscriber
    broker = from_context(provides=RabbitBroker, scope=Scope.APP)

    @provide(scope=Scope.APP)
    async def engine(self, settings: Settings) -> AsyncIterator[AsyncEngine]:
        engine = create_engine(settings.DB_URL)
        yield engine
        await engine.dispose()

    @provide(scope=Scope.APP)
    def session_factory(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        return create_session_factory(engine)

    @provide(scope=Scope.APP)
    async def http_client(self, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
        async with httpx.AsyncClient(timeout=settings.WEBHOOK_TIMEOUT) as client:
            yield client

    @provide(scope=Scope.APP)
    def gateway(self, settings: Settings) -> PaymentGateway:
        return PaymentGateway(
            success_rate=settings.GATEWAY_SUCCESS_RATE,
            min_delay=settings.GATEWAY_MIN_DELAY,
            max_delay=settings.GATEWAY_MAX_DELAY,
        )

    webhook_sender = provide(WebhookSender, scope=Scope.APP)

    # одна сессия на запрос/сообщение: её получают UoW и все репозитории
    @provide(scope=Scope.REQUEST)
    async def session(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    repositories = provide_all(
        UnitOfWork,
        PaymentQueryRepository,
        PaymentCommandRepository,
        OutboxQueryRepository,
        OutboxCommandRepository,
        scope=Scope.REQUEST,
    )


class ApplicationProvider(Provider):
    scope = Scope.REQUEST

    handlers = provide_all(
        CreatePaymentHandler,
        ChargePaymentHandler,
        CompletePaymentHandler,
        SendWebhookHandler,
        PublishOutboxHandler,
        GetPaymentHandler,
    )

    @provide
    def mediator(self, container: AsyncContainer) -> Mediator:
        return Mediator(container, HANDLERS)


def make_container(
    settings: Settings, *providers: Provider, broker: RabbitBroker | None = None
) -> AsyncContainer:
    """Единая сборка для API, consumer, relay и тестов; `providers` — переопределения.

    API брокер не нужен: без него недоступен только PublishOutboxHandler.
    """
    context: dict[type, object] = {Settings: settings}
    if broker is not None:
        context[RabbitBroker] = broker
    return make_async_container(InfraProvider(), ApplicationProvider(), *providers, context=context)
