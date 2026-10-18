"""Periodic tasks: `tq_schedules`.

A new table and nothing else, so it is safe on a live queue: no existing table is locked,
and code from before this revision never reads it.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "tq_schedules",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("task_name", sa.Text(), nullable=False),
        sa.Column("queue_name", sa.Text(), server_default="default", nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("priority", sa.SmallInteger(), nullable=True),
        sa.Column("max_attempts", sa.Integer(), nullable=True),
        sa.Column("timeout_seconds", sa.Double(), nullable=True),
        sa.Column("cron", sa.Text(), nullable=True),
        sa.Column("every_seconds", sa.Double(), nullable=True),
        sa.Column("timezone", sa.Text(), server_default="UTC", nullable=False),
        sa.Column("paused", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_task_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(cron IS NULL) <> (every_seconds IS NULL)", name=op.f("tq_schedules_timing_check")
        ),
        sa.CheckConstraint("source IN ('code', 'api')", name=op.f("tq_schedules_source_check")),
        sa.PrimaryKeyConstraint("name", name=op.f("tq_schedules_pkey")),
    )


def downgrade() -> None:
    op.drop_table("tq_schedules")
