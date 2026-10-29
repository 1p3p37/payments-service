"""Consumer `payments.new`: шлюз → статус в БД → webhook клиенту.

Запуск: `faststream run app.consumer.main:create_app --factory`.
Масштабирование: `docker compose up --scale consumer=3`.
"""

import logging
from uuid import UUID

from dishka import Provider
from dishka_faststream import FromDishka, setup_dishka
from faststream import AckPolicy, ContextRepo, FastStream
from faststream.rabbit import RabbitRouter
from pydantic import BaseModel

from app.application.commands.charge_payment import ChargePaymentCommand
from app.application.commands.complete_payment import CompletePaymentCommand
from app.application.commands.send_webhook import SendWebhookCommand
from app.application.mediator import Mediator
from app.application.queries.get_payment import GetPaymentQuery
from app.consumer.retry import RetryMiddleware
from app.infra.broker import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, create_broker, declare_topology
from app.infra.uow import UnitOfWork
from app.ioc import make_container
from app.settings import Settings, get_settings

logger = logging.getLogger(__name__)

# Router без зависимостей от окружения: брокер и настройки middleware берёт из контекста
# FastStream, поэтому импорт модуля ничего не читает и не создаёт.
router = RabbitRouter(middlewares=[RetryMiddleware])


class PaymentCreatedMessage(BaseModel):
    payment_id: UUID


@router.subscriber(PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
async def process_payment(
    body: PaymentCreatedMessage,
    uow: FromDishka[UnitOfWork],
    mediator: FromDishka[Mediator],
) -> None:
    """Только порядок шагов и границы транзакций; решения — в домене и командах.

    Шлюз вызывается между транзакциями: ждать 2–5 с с открытым соединением нельзя.
    Повторная доставка безопасна: is_pending + optimistic lock + идемпотентный шлюз.
    """
    async with uow:
        payment = await mediator.query(GetPaymentQuery(body.payment_id))

    if payment.is_pending:
        succeeded = await mediator.send(
            ChargePaymentCommand(payment.id, payment.amount, payment.currency)
        )
        async with uow:
            payment = await mediator.send(CompletePaymentCommand(payment.id, succeeded))
        logger.info("Payment %s processed: %s", payment.id, payment.status)

    await mediator.send(SendWebhookCommand(payment))


def create_app(settings: Settings | None = None, *providers: Provider) -> FastStream:
    """Собирает приложение вокруг модульного router; тесты передают свои settings и провайдеры."""
    settings = settings or get_settings()
    # FastStream настраивает только свой логгер; без этого INFO приложения теряются
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    broker = create_broker(settings)
    broker.include_router(router)
    # settings в контексте — для RetryMiddleware (брокер кладёт себя туда сам)
    app = FastStream(broker, context=ContextRepo({"settings": settings}))

    container = make_container(settings, *providers, broker=broker)
    # REQUEST-скоуп dishka = одно сообщение = одна сессия БД
    setup_dishka(container, app, auto_inject=True, finalize_container=True)

    @app.after_startup
    async def declare() -> None:
        await declare_topology(broker, settings)

    return app
