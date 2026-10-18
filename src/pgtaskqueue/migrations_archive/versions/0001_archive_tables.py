"""The archive: finished tasks and their log lines, moved out of the queue's database.

Two new, empty tables, so plain `CREATE INDEX` is fine: nothing reads or writes them yet.
Their columns are those of `tq_tasks` and `tq_task_logs` at revision 0003 of the queue's
schema, plus `tq_archive_tasks.archived_at`.

Revision ID: 0001
Revises:
Create Date: 2026-10-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None

_TS = sa.DateTime(timezone=True)
_JSON = postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "tq_archive_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("queue_name", sa.Text(), server_default="default", nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("priority", sa.SmallInteger(), server_default="100", nullable=False),
        sa.Column("payload", _JSON, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("metadata", _JSON, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("progress", _JSON, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("output", _JSON, nullable=True),
        sa.Column("error", _JSON, nullable=True),
        sa.Column("step", sa.Text(), nullable=True),
        sa.Column("attempt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="3", nullable=False),
        sa.Column("timeout_seconds", sa.Double(), nullable=True),
        sa.Column("run_after", _TS, server_default=sa.text("now()"), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=True),
        sa.Column("callback", _JSON, nullable=True),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("capped", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("worker_id", sa.Text(), nullable=True),
        sa.Column("spent_seconds", sa.Double(), server_default="0", nullable=False),
        sa.Column("created_at", _TS, server_default=sa.text("now()"), nullable=False),
        sa.Column("started_at", _TS, nullable=True),
        sa.Column("heartbeat_at", _TS, nullable=True),
        sa.Column("completed_at", _TS, nullable=True),
        sa.Column("archived_at", _TS, server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed', 'canceled')", name="tq_archive_tasks_status_check"
        ),
        sa.PrimaryKeyConstraint("id", name="tq_archive_tasks_pkey"),
    )
    op.create_index("tq_archive_tasks_completed_idx", "tq_archive_tasks", ["completed_at"])
    op.create_index("tq_archive_tasks_name_idx", "tq_archive_tasks", ["name", "id"])
    op.create_index(
        "tq_archive_tasks_created_by_idx",
        "tq_archive_tasks",
        ["created_by", "id"],
        postgresql_where=sa.text("created_by IS NOT NULL"),
    )

    op.create_table(
        "tq_archive_task_logs",
        sa.Column("id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("attempt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("level", sa.Text(), server_default="info", nullable=False),
        sa.Column("step", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("data", _JSON, nullable=True),
        sa.Column("created_at", _TS, server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tq_archive_tasks.id"],
            name="tq_archive_task_logs_task_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="tq_archive_task_logs_pkey"),
    )
    op.create_index("tq_archive_task_logs_task_idx", "tq_archive_task_logs", ["task_id", "id"])


def downgrade() -> None:
    op.drop_table("tq_archive_task_logs")
    op.drop_table("tq_archive_tasks")
