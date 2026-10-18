"""Account limits, the manager role, and held tasks.

* `tq_users`: four nullable limit columns and the `manager` role. A small table; the
  constraint change locks it for a moment.
* `tq_tasks.capped`: a column with a constant default, which PostgreSQL adds without
  rewriting the table. The lock it needs is brief, but it waits behind any long transaction
  on `tq_tasks`, so `lock_timeout` makes it give up rather than stall claims behind it.
* Indexes on `tq_tasks`, built `CONCURRENTLY` outside the transaction. The promoter's index
  is replaced by one that leaves held tasks out.

Code from before this revision ignores the new columns. During a rolling deploy an old
worker's promoter can release a held task early; that matters only once an account has a
running limit, which nothing sets until the new code is out.

Every statement is idempotent: `autocommit_block()` commits the transaction holding the
lock `tq migrate` takes, so a second run started at the same moment may run this one too.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-14
"""

from __future__ import annotations

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | None = None
depends_on: str | None = None

_LIMITS = ("max_waiting", "max_running", "enqueue_per_minute", "min_priority")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    for column in _LIMITS:
        op.execute(f"ALTER TABLE tq_users ADD COLUMN IF NOT EXISTS {column} integer")
    op.execute("ALTER TABLE tq_users DROP CONSTRAINT IF EXISTS tq_users_limits_check")
    op.execute(
        "ALTER TABLE tq_users ADD CONSTRAINT tq_users_limits_check CHECK ("
        "max_waiting >= 0 AND max_running >= 0 AND enqueue_per_minute >= 0 "
        "AND min_priority BETWEEN 0 AND 32767)"
    )
    op.execute("ALTER TABLE tq_users DROP CONSTRAINT IF EXISTS tq_users_role_check")
    op.execute(
        "ALTER TABLE tq_users ADD CONSTRAINT tq_users_role_check CHECK ("
        "role IN ('admin', 'manager', 'operator', 'submitter', 'viewer', 'worker'))"
    )
    op.execute(
        "ALTER TABLE tq_tasks ADD COLUMN IF NOT EXISTS capped boolean DEFAULT false NOT NULL"
    )

    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS tq_tasks_due_idx ON tq_tasks (run_after) "
            "WHERE status = 'scheduled' AND NOT capped"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS tq_tasks_held_idx "
            "ON tq_tasks (created_by, run_after, id) WHERE status = 'scheduled' AND capped"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS tq_tasks_active_by_idx "
            "ON tq_tasks (created_by, status) "
            "WHERE created_by IS NOT NULL AND status IN ('scheduled', 'pending', 'running')"
        )
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS tq_tasks_scheduled_idx")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS tq_tasks_scheduled_idx "
            "ON tq_tasks (run_after) WHERE status = 'scheduled'"
        )
        for index in ("tq_tasks_due_idx", "tq_tasks_held_idx", "tq_tasks_active_by_idx"):
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index}")

    op.execute("SET LOCAL lock_timeout = '10s'")
    # Held tasks become ordinary scheduled ones, which the promoter then releases.
    op.execute("ALTER TABLE tq_tasks DROP COLUMN IF EXISTS capped")
    # The old constraint has no `manager`. The nearest role it knows without account
    # management.
    op.execute("UPDATE tq_users SET role = 'operator' WHERE role = 'manager'")
    op.execute("ALTER TABLE tq_users DROP CONSTRAINT IF EXISTS tq_users_role_check")
    op.execute(
        "ALTER TABLE tq_users ADD CONSTRAINT tq_users_role_check CHECK ("
        "role IN ('admin', 'operator', 'submitter', 'viewer', 'worker'))"
    )
    op.execute("ALTER TABLE tq_users DROP CONSTRAINT IF EXISTS tq_users_limits_check")
    for column in _LIMITS:
        op.execute(f"ALTER TABLE tq_users DROP COLUMN IF EXISTS {column}")
