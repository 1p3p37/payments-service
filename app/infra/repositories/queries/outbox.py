from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.models import OutboxModel


class OutboxQueryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def fetch_for_update(self, limit: int, max_attempts: int) -> list[OutboxModel]:
        """Старые события первыми; строки заблокированы до конца транзакции.

        SKIP LOCKED: параллельные relay берут разные строки. Сообщения, исчерпавшие
        `max_attempts`, пропускаются, чтобы не блокировать новые (остаются в таблице
        для разбора и ручной переотправки).
        """
        stmt = (
            select(OutboxModel)
            .where(OutboxModel.attempts < max_attempts)
            .order_by(OutboxModel.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.execute(stmt)).scalars())
