from dataclasses import dataclass

from app.application.mediator import Command
from app.domain.payment import Payment
from app.infra.webhook import WebhookSender


@dataclass(frozen=True, slots=True)
class SendWebhookCommand(Command[None]):
    payment: Payment


@dataclass
class SendWebhookHandler:
    webhook_sender: WebhookSender

    async def handle(self, command: SendWebhookCommand) -> None:
        await self.webhook_sender.send(command.payment)
