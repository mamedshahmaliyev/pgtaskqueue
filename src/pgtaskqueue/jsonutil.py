"""Turning what an executor hands back into something a `jsonb` column accepts."""

from __future__ import annotations

import traceback
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Any
from uuid import UUID

from pydantic import BaseModel


def to_jsonable(value: Any) -> Any:
    """`value` as plain JSON types, with NUL characters removed.

    Postgres refuses `\\u0000` inside `text` and `jsonb`, and it turns up in exactly the
    places a queue stores — a subprocess's stderr, a scraped page — so it is stripped here
    rather than left to fail the write that records why a task failed.
    """
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return value.replace("\x00", "") if "\x00" in value else value
    if isinstance(value, BaseModel):
        return to_jsonable(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [to_jsonable(v) for v in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, UUID | PurePath | Decimal):
        return str(value)
    if isinstance(value, Enum):
        return to_jsonable(value.value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").replace("\x00", "")
    if is_dataclass(value) and not isinstance(value, type):
        return to_jsonable(asdict(value))
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def clip(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def error_dict(exc: BaseException, *, attempt: int, worker_id: str | None = None) -> dict:
    """What is stored in `tq_tasks.error` for a failed attempt."""
    trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return to_jsonable(
        {
            "type": type(exc).__name__,
            "message": clip(str(exc), 4000),
            "traceback": clip(trace, 20000),
            "attempt": attempt,
            "worker": worker_id,
        }
    )
