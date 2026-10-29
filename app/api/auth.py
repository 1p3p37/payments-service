import secrets
from typing import Annotated

from fastapi import Header, HTTPException, Request, status


async def verify_api_key(
    request: Request,
    # Header необязательный: без ключа должен быть 401, а не 422 валидации
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    expected: str = request.app.state.settings.API_KEY
    if x_api_key is None or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
