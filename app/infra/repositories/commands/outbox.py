from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import bindparam, delete, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.base import BaseEvent
from app.infra.models import OutboxModel

outbox_table = OutboxModel.__table__


class OutboxCommandRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add_events(self, events: Sequence[BaseEvent]) -> None:
        if not events:
            return
        now = datetime.now(UTC)
        rows = [
            {
                "id": e.event_id,
                "event_type": e.event_name,
                "payload": e.to_dict(),
                "created_at": now,
                "attempts": 0,
                "last_error": None,
            }
            for e in events
        ]
        await self.session.execute(pg_insert(OutboxModel).values(rows))

    async def delete(self, ids: Sequence[UUID]) -> None:
        if not ids:
            return
        stmt = (
            delete(OutboxModel)
            .where(OutboxModel.id.in_(ids))
            .execution_options(synchronize_session=False)
        )
        await self.session.execute(stmt)

    async def record_failures(self, errors: Mapping[UUID, str]) -> None:
        """attempts += 1 и причина отказа — для каждого сообщения своя (executemany)."""
        if not errors:
            return
        stmt = (
            update(outbox_table)
            .where(outbox_table.c.id == bindparam("row_id"))
            .values(attempts=outbox_table.c.attempts + 1, last_error=bindparam("error"))
        )
        await self.session.execute(
            stmt, [{"row_id": row_id, "error": error} for row_id, error in errors.items()]
        )
