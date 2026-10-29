import httpx

from app.domain.payment import Payment
from app.infra.exceptions import WebhookDeliveryError


class WebhookSender:
    """At-least-once: получатель дедуплицирует повторы по `X-Event-Id`."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def send(self, payment: Payment) -> None:
        payload = {
            "payment_id": str(payment.id),
            "status": payment.status,
            "amount": str(payment.amount),
            "currency": payment.currency,
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
            "metadata": payment.metadata,
        }
        try:
            response = await self.client.post(
                payment.webhook_url, json=payload, headers={"X-Event-Id": str(payment.id)}
            )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(payment.webhook_url, repr(exc)) from exc
        if not response.is_success:
            raise WebhookDeliveryError(payment.webhook_url, f"HTTP {response.status_code}")
