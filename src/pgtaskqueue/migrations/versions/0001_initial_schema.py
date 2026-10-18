"""Initial schema: tasks, their logs, task types, queues, workers and users.

The comments that explain each index are on the models (`pgtaskqueue/models.py`); this file
is the frozen record of what was created.

Revision ID: 0001
Revises:
Create Date: 2026-10-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "tq_queues",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("paused", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("name", name=op.f("tq_queues_pkey")),
    )
    op.create_table(
        "tq_task_types",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "payload_schema",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("max_attempts", sa.Integer(), nullable=True),
        sa.Column("timeout_seconds", sa.Double(), nullable=True),
        sa.Column("registered_by", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("name", name=op.f("tq_task_types_pkey")),
    )
    op.create_table(
        "tq_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("queue_name", sa.Text(), server_default="default", nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("priority", sa.SmallInteger(), server_default="100", nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "progress",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "output", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            "error", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True
        ),
        sa.Column("step", sa.Text(), nullable=True),
        sa.Column("attempt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="3", nullable=False),
        sa.Column("timeout_seconds", sa.Double(), nullable=True),
        sa.Column(
            "run_after", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("dedupe_key", sa.Text(), nullable=True),
        sa.Column(
            "callback", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True
        ),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("worker_id", sa.Text(), nullable=True),
        sa.Column("spent_seconds", sa.Double(), server_default="0", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('scheduled', 'pending', 'running', 'succeeded', 'failed', 'canceled')",
            name=op.f("tq_tasks_status_check"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("tq_tasks_pkey")),
    )
    op.create_index(
        "tq_tasks_claim_idx",
        "tq_tasks",
        ["queue_name", "name", "priority", "run_after", "id"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "tq_tasks_completed_idx",
        "tq_tasks",
        ["completed_at"],
        unique=False,
        postgresql_where=sa.text("completed_at IS NOT NULL"),
    )
    op.create_index(
        "tq_tasks_created_by_idx",
        "tq_tasks",
        ["created_by", "id"],
        unique=False,
        postgresql_where=sa.text("created_by IS NOT NULL"),
    )
    op.create_index(
        "tq_tasks_dedupe_idx",
        "tq_tasks",
        ["queue_name", "name", "dedupe_key"],
        unique=True,
        postgresql_where=sa.text(
            "dedupe_key IS NOT NULL AND status IN ('scheduled', 'pending', 'running')"
        ),
    )
    op.create_index("tq_tasks_name_idx", "tq_tasks", ["name", "id"], unique=False)
    op.create_index(
        "tq_tasks_running_idx",
        "tq_tasks",
        ["worker_id"],
        unique=False,
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index(
        "tq_tasks_scheduled_idx",
        "tq_tasks",
        ["run_after"],
        unique=False,
        postgresql_where=sa.text("status = 'scheduled'"),
    )
    op.create_index("tq_tasks_status_idx", "tq_tasks", ["status", "id"], unique=False)
    # A queue table is rewritten constantly. Room on each page for HOT updates, and an
    # autovacuum that visits long before the default 20% of the table is dead.
    op.execute(
        "ALTER TABLE tq_tasks SET ("
        "fillfactor = 85, "
        "autovacuum_vacuum_scale_factor = 0.02, "
        "autovacuum_analyze_scale_factor = 0.02, "
        "autovacuum_vacuum_cost_limit = 2000)"
    )
    op.create_table(
        "tq_users",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("username", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), server_default="user", nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column(
            "queues",
            postgresql.ARRAY(sa.Text()),
            server_default=sa.text("'{*}'::text[]"),
            nullable=False,
        ),
        sa.Column(
            "task_names",
            postgresql.ARRAY(sa.Text()),
            server_default=sa.text("'{*}'::text[]"),
            nullable=False,
        ),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("token_version", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("kind IN ('user', 'service')", name=op.f("tq_users_kind_check")),
        sa.CheckConstraint(
            "role IN ('admin', 'operator', 'submitter', 'viewer', 'worker')",
            name=op.f("tq_users_role_check"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("tq_users_pkey")),
        sa.UniqueConstraint("username", name=op.f("tq_users_username_key")),
    )
    op.create_table(
        "tq_workers",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("queue_name", sa.Text(), nullable=False),
        sa.Column("hostname", sa.Text(), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column(
            "pools",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("slots", sa.Integer(), server_default="0", nullable=False),
        sa.Column("in_flight", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "counters",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("version", sa.Text(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "heartbeat_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("tq_workers_pkey")),
    )
    op.create_index("tq_workers_heartbeat_idx", "tq_workers", ["heartbeat_at"], unique=False)
    op.create_table(
        "tq_task_logs",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("attempt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("level", sa.Text(), server_default="info", nullable=False),
        sa.Column("step", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column(
            "data", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["task_id"], ["tq_tasks.id"], name=op.f("tq_task_logs_task_id_fkey"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("tq_task_logs_pkey")),
    )
    op.create_index("tq_task_logs_task_idx", "tq_task_logs", ["task_id", "id"], unique=False)


def downgrade() -> None:
    op.drop_index("tq_task_logs_task_idx", table_name="tq_task_logs")
    op.drop_table("tq_task_logs")
    op.drop_index("tq_workers_heartbeat_idx", table_name="tq_workers")
    op.drop_table("tq_workers")
    op.drop_table("tq_users")
    op.drop_index("tq_tasks_status_idx", table_name="tq_tasks")
    op.drop_index(
        "tq_tasks_scheduled_idx",
        table_name="tq_tasks",
        postgresql_where=sa.text("status = 'scheduled'"),
    )
    op.drop_index(
        "tq_tasks_running_idx",
        table_name="tq_tasks",
        postgresql_where=sa.text("status = 'running'"),
    )
    op.drop_index("tq_tasks_name_idx", table_name="tq_tasks")
    op.drop_index(
        "tq_tasks_dedupe_idx",
        table_name="tq_tasks",
        postgresql_where=sa.text(
            "dedupe_key IS NOT NULL AND status IN ('scheduled', 'pending', 'running')"
        ),
    )
    op.drop_index(
        "tq_tasks_created_by_idx",
        table_name="tq_tasks",
        postgresql_where=sa.text("created_by IS NOT NULL"),
    )
    op.drop_index(
        "tq_tasks_completed_idx",
        table_name="tq_tasks",
        postgresql_where=sa.text("completed_at IS NOT NULL"),
    )
    op.drop_index(
        "tq_tasks_claim_idx", table_name="tq_tasks", postgresql_where=sa.text("status = 'pending'")
    )
    op.drop_table("tq_tasks")
    op.drop_table("tq_task_types")
    op.drop_table("tq_queues")
