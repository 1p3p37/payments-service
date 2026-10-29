"""e2e на поднятом `docker compose up`. Без стека тесты пропускаются."""

import os
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

API_URL = os.getenv("E2E_API_URL", "http://localhost:8000")
MOCK_URL = os.getenv("E2E_WEBHOOK_MOCK_URL", "http://localhost:8001")
RABBIT_API_URL = os.getenv("E2E_RABBITMQ_API_URL", "http://localhost:15672/api")
API_KEY = os.getenv("API_KEY", "dev-api-key")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if Path(item.fspath).is_relative_to(Path(__file__).parent):
            item.add_marker(pytest.mark.e2e)


@pytest.fixture
async def api() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=API_URL, headers={"X-API-Key": API_KEY}, timeout=10
    ) as client:
        try:
            await client.get("/health")
        except httpx.HTTPError:
            pytest.skip(f"Stack is not running at {API_URL}: docker compose up -d")
        yield client


@pytest.fixture
async def mock() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=MOCK_URL, timeout=10) as client:
        yield client
        await client.put("/config", json={"fail_rate": 0})  # не оставлять ошибки включёнными


@pytest.fixture
async def rabbit() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=RABBIT_API_URL, auth=("guest", "guest")) as client:
        yield client
