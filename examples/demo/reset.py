"""Keeps a public demo fresh: a full reset every hour, the demo accounts repaired every minute.

    DEMO_RESET_DATABASE=task_queue python -m examples.demo.reset
    DEMO_RESET_DATABASE=task_queue python -m examples.demo.reset --once   # one reset, then exit

The reset EMPTIES THE QUEUE and removes every account that is not an admin or a worker
(examples/demo/seed.py), so it refuses to start unless DEMO_RESET_DATABASE names the
database TQ_DATABASE_URL points at. It resets once at start, so starting or restarting it
resets at once. Run one of these per demo, next to the workers; the compose service on the
demo host is in docs/demo.md.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

from sqlalchemy.engine import make_url

from examples.demo import seed
from pgtaskqueue.db import Database
from pgtaskqueue.settings import get_settings

log = logging.getLogger("demo.reset")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m examples.demo.reset", description=__doc__)
    parser.add_argument("--every", type=float, default=3600, help="seconds between resets")
    parser.add_argument(
        "--repair-every", type=float, default=60, help="seconds between account repairs"
    )
    parser.add_argument("--once", action="store_true", help="reset once and exit")
    return parser


async def run(db: Database, every: float, repair_every: float, once: bool) -> None:
    next_reset = 0.0
    while True:
        now = time.monotonic()
        try:
            if now >= next_reset:
                log.info("reset: %s", await seed.reset(db))
                next_reset = now + every
            else:
                repaired = await seed.repair(db)
                if any(repaired.values()):
                    log.info("repair: %s", repaired)
        except Exception:
            # The database restarting, or the reset's lock timing out behind a long claim:
            # both pass. Try again at the next tick rather than exit and lose the schedule.
            log.exception("demo reset failed; trying again in %g s", repair_every)
        if once:
            return
        await asyncio.sleep(max(0.0, min(repair_every, next_reset - time.monotonic())))


async def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = get_settings()
    database = make_url(settings.database_url).database
    if os.environ.get("DEMO_RESET_DATABASE") != database:
        print(
            f"this empties the queue in the database {database!r}. Set "
            f"DEMO_RESET_DATABASE={database} to confirm that is the demo's.",
            file=sys.stderr,
        )
        return 2
    db = Database(settings=settings, application_name="tq-demo-reset")
    try:
        await run(db, args.every, args.repair_every, args.once)
    finally:
        await db.dispose()
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    sys.exit(asyncio.run(main()))
