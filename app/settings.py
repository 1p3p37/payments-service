from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    DB_URL: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    RABBIT_URL: str = "amqp://guest:guest@localhost:5672/"
    API_KEY: str = "dev-api-key"
    # Swagger/OpenAPI открыты без ключа (браузер не шлёт заголовок при загрузке схемы),
    # но запросы из Swagger к API — только с ключом через Authorize. В проде — выключить
    API_DOCS_ENABLED: bool = True

    MAX_RETRIES: int = 3
    RETRY_BASE_DELAY: float = 1  # секунды, задержка попытки n = base * 2^(n-1)

    OUTBOX_BATCH_SIZE: int = 1000
    OUTBOX_PUBLISH_CONCURRENCY: int = 100
    OUTBOX_MAX_ATTEMPTS: int = 5  # отказов брокера, после которых сообщение пропускается
    OUTBOX_POLL_INTERVAL: float = 0.5  # секунды, пауза relay, когда продвинуться не удалось
    OUTBOX_SHUTDOWN_TIMEOUT: float = 30

    GATEWAY_SUCCESS_RATE: float = 0.9
    GATEWAY_MIN_DELAY: float = 2
    GATEWAY_MAX_DELAY: float = 5

    WEBHOOK_TIMEOUT: float = 5


@lru_cache
def get_settings() -> Settings:
    return Settings()
