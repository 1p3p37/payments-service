"""Мок получателя webhook'ов для демо и e2e. Не часть сервиса.

Запуск: `uvicorn app.dev.webhook_mock:app --port 8001`.
- `POST /hook` — с вероятностью `fail_rate` отвечает 500 (проверка retry/DLQ);
- `GET /received` — принятые уведомления (дубли по `X-Event-Id` отмечены);
- `PUT /config` — поменять `fail_rate` без перезапуска.
"""

import logging
import os
import random
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Header, Request, Response, status
from pydantic import BaseModel, Field

logger = logging.getLogger("webhook_mock")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

app = FastAPI(title="Webhook mock")


@dataclass
class State:
    fail_rate: float
    received: list[dict[str, Any]] = field(default_factory=list)


state = State(fail_rate=float(os.getenv("WEBHOOK_FAIL_RATE", "0")))


class Config(BaseModel):
    fail_rate: float = Field(ge=0, le=1)


@app.post("/hook")
async def hook(request: Request, x_event_id: str = Header("")) -> Response:
    payload = await request.json()
    if random.random() < state.fail_rate:  # noqa: S311 — имитация сбоев, не криптография
        logger.warning("FAIL  %s %s", x_event_id, payload.get("status"))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    duplicate = any(r["event_id"] == x_event_id for r in state.received)
    state.received.append({"event_id": x_event_id, "duplicate": duplicate, "payload": payload})
    logger.info("OK    %s %s%s", x_event_id, payload.get("status"), " (duplicate)" * duplicate)
    return Response(status_code=status.HTTP_200_OK)


@app.get("/received")
async def received() -> list[dict[str, Any]]:
    return state.received


@app.put("/config")
async def configure(config: Config) -> Config:
    state.fail_rate = config.fail_rate
    logger.info("fail_rate = %s", config.fail_rate)
    return config


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
