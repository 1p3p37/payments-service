from app.application.commands.charge_payment import ChargePaymentCommand, ChargePaymentHandler
from app.application.commands.complete_payment import (
    CompletePaymentCommand,
    CompletePaymentHandler,
)
from app.application.commands.create_payment import CreatePaymentCommand, CreatePaymentHandler
from app.application.commands.publish_outbox import PublishOutboxCommand, PublishOutboxHandler
from app.application.commands.send_webhook import SendWebhookCommand, SendWebhookHandler
from app.application.queries.get_payment import GetPaymentHandler, GetPaymentQuery

# Отдельный модуль, чтобы не было цикла mediator ↔ хендлеры.
# Новый хендлер: добавить сюда и в ApplicationProvider (app/ioc.py).
HANDLERS: dict[type, type] = {
    CreatePaymentCommand: CreatePaymentHandler,
    ChargePaymentCommand: ChargePaymentHandler,
    CompletePaymentCommand: CompletePaymentHandler,
    SendWebhookCommand: SendWebhookHandler,
    PublishOutboxCommand: PublishOutboxHandler,
    GetPaymentQuery: GetPaymentHandler,
}
