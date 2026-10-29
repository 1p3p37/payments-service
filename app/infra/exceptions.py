from uuid import UUID


class ConcurrentUpdateError(Exception):
    def __init__(self, payment_id: UUID) -> None:
        self.payment_id = payment_id
        super().__init__(f"Payment {payment_id} was modified concurrently")


class WebhookDeliveryError(Exception):
    def __init__(self, url: str, reason: str) -> None:
        self.url = url
        self.reason = reason
        super().__init__(f"Webhook delivery to {url} failed: {reason}")
