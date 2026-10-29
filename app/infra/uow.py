from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession


class UnitOfWork:
    """Граница транзакции: commit при успехе, rollback при исключении.

    Сессию создаёт и закрывает DI-контейнер (одна на запрос/сообщение), её же получают
    репозитории. Транзакция стартует автоматически на первом запросе, поэтому повторный
    `async with uow:` на той же сессии — новая транзакция.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            await self.session.commit()
        else:
            await self.session.rollback()
