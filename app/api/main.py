"""HTTP API. Запуск: `uvicorn app.api.main:create_app --factory`."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from dishka import Provider
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.api.routes import router
from app.domain.exceptions import IdempotencyKeyConflict, InvalidPaymentAmount, PaymentNotFound
from app.ioc import make_container
from app.settings import Settings, get_settings

ERROR_STATUS = {
    PaymentNotFound: status.HTTP_404_NOT_FOUND,
    IdempotencyKeyConflict: status.HTTP_409_CONFLICT,
    InvalidPaymentAmount: status.HTTP_422_UNPROCESSABLE_CONTENT,
}


def create_app(settings: Settings | None = None, *providers: Provider) -> FastAPI:
    """Фабрика: тесты передают свои settings и провайдеры-переопределения."""
    settings = settings or get_settings()
    container = make_container(settings, *providers)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await container.close()

    app = FastAPI(title="Payments Service", version="1.0.0", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(router)
    setup_dishka(container, app)

    async def domain_error(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(status_code=ERROR_STATUS[type(exc)], content={"detail": str(exc)})

    for error in ERROR_STATUS:
        app.add_exception_handler(error, domain_error)

    @app.get("/health", tags=["service"])
    async def health() -> dict[str, str]:
        """Без X-API-Key: используется healthcheck'ом docker compose."""
        return {"status": "ok"}

    return app
