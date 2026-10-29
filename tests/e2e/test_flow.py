from uuid import uuid4

import httpx

from tests.integration.helpers import wait_until

# адрес mock'а изнутри docker-сети: webhook отправляет consumer из контейнера
WEBHOOK_URL = "http://webhook-mock:8001/hook"


async def create_payment(api: httpx.AsyncClient) -> str:
    response = await api.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": f"e2e-{uuid4()}"},
        json={
            "amount": "42.00",
            "currency": "EUR",
            "description": "e2e",
            "metadata": {"source": "e2e"},
            "webhook_url": WEBHOOK_URL,
        },
    )
    assert response.status_code == 202, response.text
    return response.json()["payment_id"]


async def test_payment_is_processed_and_client_notified(
    api: httpx.AsyncClient, mock: httpx.AsyncClient
):
    payment_id = await create_payment(api)

    async def processed() -> dict | None:
        payment = (await api.get(f"/api/v1/payments/{payment_id}")).json()
        return payment if payment["status"] != "pending" else None

    payment = await wait_until(processed, timeout=20, interval=0.5)

    async def notified() -> dict | None:
        received = (await mock.get("/received")).json()
        return next((r for r in received if r["event_id"] == payment_id), None)

    webhook = await wait_until(notified, timeout=10, interval=0.5)
    assert payment["status"] in {"succeeded", "failed"}
    assert payment["processed_at"] is not None
    assert webhook["payload"]["status"] == payment["status"]


async def test_undeliverable_webhook_ends_in_dlq(
    api: httpx.AsyncClient, mock: httpx.AsyncClient, rabbit: httpx.AsyncClient
):
    async def dlq_size() -> int:
        return (await rabbit.get("/queues/%2F/payments.new.dlq")).json()["messages"]

    before = await dlq_size()
    await mock.put("/config", json={"fail_rate": 1})

    payment_id = await create_payment(api)

    async def dead_lettered() -> bool:
        return await dlq_size() > before

    # шлюз 2–5 с + retry 1/2/4 с + обновление статистики management API
    await wait_until(dead_lettered, timeout=40, interval=1)
    # статус платежа сохранён, несмотря на недоставленный webhook
    payment = (await api.get(f"/api/v1/payments/{payment_id}")).json()
    assert payment["status"] in {"succeeded", "failed"}
