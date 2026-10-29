"""outbox: attempts и last_error для изоляции poison-сообщений

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-29 20:59:08.858829

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # server_default только чтобы заполнить уже существующие строки (NOT NULL на непустой
    # таблице); дальше значение ставит код, как и для остальных колонок
    op.add_column("outbox", sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"))
    op.alter_column("outbox", "attempts", server_default=None)
    op.add_column("outbox", sa.Column("last_error", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("outbox", "last_error")
    op.drop_column("outbox", "attempts")
