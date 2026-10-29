import asyncio
from collections.abc import AsyncIterator, Awaitable
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.api.main import create_app
from app.infra.models import OutboxModel, PaymentModel
from app.settings import Settings
from tests.integration.conftest import InstantGatewayProvider

API_KEY = "test-key"
URL = "/api/v1/payments"
BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "http://client.test/hook",
}


@pytest.fixture
async def client(settings: Settings, engine: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings.model_copy(update={"API_KEY": API_KEY}), InstantGatewayProvider())
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=transport, base_url="http://test", headers={"X-API-Key": API_KEY}
        ) as client,
    ):
        yield client


def create(client: httpx.AsyncClient, key: str = "key-1", **body) -> Awaitable[httpx.Response]:
    return client.post(URL, json=BODY | body, headers={"Idempotency-Key": key})


async def counts(session_factory: async_sessionmaker[AsyncSession]) -> tuple[int, int]:
    async with session_factory() as session:
        payments = await session.scalar(select(func.count()).select_from(PaymentModel))
        outbox = await session.scalar(select(func.count()).select_from(OutboxModel))
    return payments, outbox


class TestAuth:
    @pytest.mark.parametrize("headers", [{"X-API-Key": ""}, {"X-API-Key": "wrong"}])
    async def test_wrong_key(self, client: httpx.AsyncClient, headers: dict):
        response = await client.get(f"{URL}/{uuid4()}", headers=headers)

        assert response.status_code == 401

    async def test_missing_key_is_401_not_422(self, client: httpx.AsyncClient):
        del client.headers["X-API-Key"]

        # без ключа и без Idempotency-Key: проверка ключа срабатывает раньше валидации
        response = await client.post(URL, json={})

        assert response.status_code == 401

    async def test_health_without_key(self, client: httpx.AsyncClient):
        del client.headers["X-API-Key"]

        response = await client.get("/health")

        assert response.status_code == 200


class TestCreatePayment:
    async def test_accepted(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ):
        response = await create(client)

        assert response.status_code == 202
        assert set(response.json()) == {"payment_id", "status", "created_at"}
        assert response.json()["status"] == "pending"
        assert await counts(session_factory) == (1, 1)

    async def test_repeat_returns_same_response(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ):
        first = await create(client)
        repeat = await create(client)

        assert repeat.status_code == 202
        assert repeat.json() == first.json()
        assert await counts(session_factory) == (1, 1)

    async def test_same_key_different_body(self, client: httpx.AsyncClient):
        await create(client)

        response = await create(client, amount="200.00")

        assert response.status_code == 409
        assert "key-1" in response.json()["detail"]

    async def test_concurrent_duplicates(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ):
        responses = await asyncio.gather(*(create(client) for _ in range(5)))

        assert {r.status_code for r in responses} == {202}
        assert len({r.json()["payment_id"] for r in responses}) == 1
        assert await counts(session_factory) == (1, 1)

    async def test_missing_idempotency_key(self, client: httpx.AsyncClient):
        response = await client.post(URL, json=BODY)

        assert response.status_code == 422

    @pytest.mark.parametrize(
        "override",
        [
            {"amount": "0"},
            {"amount": "-1"},
            {"amount": "10.001"},
            {"currency": "GBP"},
            {"webhook_url": "not-a-url"},
            {"description": "x" * 256},
        ],
    )
    async def test_invalid_body(self, client: httpx.AsyncClient, override: dict):
        response = await create(client, **override)

        assert response.status_code == 422


class TestGetPayment:
    async def test_found(self, client: httpx.AsyncClient):
        created = (await create(client)).json()

        response = await client.get(f"{URL}/{created['payment_id']}")

        assert response.status_code == 200
        assert response.json() == {
            "payment_id": created["payment_id"],
            "amount": "100.50",
            "currency": "RUB",
            "description": "Order #1",
            "metadata": {"order_id": 1},
            "status": "pending",
            "idempotency_key": "key-1",
            "webhook_url": "http://client.test/hook",
            "created_at": created["created_at"],
            "processed_at": None,
        }

    async def test_not_found(self, client: httpx.AsyncClient):
        response = await client.get(f"{URL}/{uuid4()}")

        assert response.status_code == 404

    async def test_invalid_uuid(self, client: httpx.AsyncClient):
        response = await client.get(f"{URL}/not-a-uuid")

        assert response.status_code == 422
