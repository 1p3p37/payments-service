import secrets
from typing import Annotated

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

# auto_error=False: без ключа нужен 401, а не 403/422 по умолчанию;
# security-схема даёт кнопку Authorize в Swagger
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    request: Request, api_key: Annotated[str | None, Security(api_key_header)]
) -> None:
    expected: str = request.app.state.settings.API_KEY
    if api_key is None or not secrets.compare_digest(api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
