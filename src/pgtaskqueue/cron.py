"""Cron expressions: parsing one, and finding the next time it fires.

    Cron.parse("*/15 9-17 * * mon-fri").after(now, ZoneInfo("Europe/Berlin"))

The five standard fields (minute, hour, day of month, month, day of week) with lists,
ranges, steps, month and weekday names, and the `@hourly` family of aliases. Not supported:
a seconds field, `@reboot`, and the `L`, `W`, `#` and `?` extensions. Written here rather
than taken from a library because every worker image installs the core dependencies, and
this is the whole of what a schedule needs.

Two rules are worth knowing, and both are cron's own:

* When day of month **and** day of week are both restricted, a day matches if **either**
  does: `0 0 13 * fri` is every 13th and every Friday.
* Around a daylight-saving change, in the schedule's timezone: a wall-clock time that is
  skipped fires at the same instant it would have without the change (02:30 becomes 03:30),
  and one that happens twice fires the first time only, unless the hour field is a
  wildcard, in which case the schedule is "every so often" and fires both times.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, tzinfo

_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
_MONTHS = {
    name: number
    for number, name in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), 1
    )
}
_WEEKDAYS = {
    name: number for number, name in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))
}
# (what an error calls the field, lowest, highest, names accepted in place of numbers)
_FIELDS: tuple[tuple[str, int, int, dict[str, int]], ...] = (
    ("minute", 0, 59, {}),
    ("hour", 0, 23, {}),
    ("day of month", 1, 31, {}),
    ("month", 1, 12, _MONTHS),
    # 7 is Sunday too, as in every crontab.
    ("day of week", 0, 7, _WEEKDAYS),
)
# How far ahead to look for a fire time. `0 0 29 2 *` can wait eight years (2096 to 2104,
# because 2100 is not a leap year); an expression with nothing in that span never fires.
_HORIZON_DAYS = 3000
_DAY = timedelta(days=1)


def _field(text: str, label: str, low: int, high: int, names: dict[str, int]) -> frozenset[int]:
    def value(token: str) -> int:
        if token.isdigit():
            return int(token)
        if token.lower() in names:
            return names[token.lower()]
        raise ValueError(f"{label}: {token!r} is not a value")

    values: set[int] = set()
    for part in text.split(","):
        span, slash, step_text = part.partition("/")
        if slash and not (step_text.isdigit() and int(step_text) > 0):
            raise ValueError(f"{label}: the step in {part!r} must be a positive number")
        step = int(step_text) if slash else 1
        if span == "*":
            first, last = low, high
        else:
            start, dash, end = span.partition("-")
            first = value(start)
            # `5/15` reads as "from 5 on, every 15".
            last = value(end) if dash else (high if slash else first)
        if not low <= first <= last <= high:
            raise ValueError(f"{label}: {part!r} is outside {low}-{high}")
        values.update(range(first, last + 1, step))
    return frozenset(values)


@dataclass(frozen=True, slots=True)
class Cron:
    expression: str
    minutes: tuple[int, ...]
    hours: tuple[int, ...]
    days: frozenset[int]
    months: frozenset[int]
    # 0 is Sunday.
    weekdays: frozenset[int]
    # Whether the field was written starting with `*`. Cron's either-or rule for the two day
    # fields, and the daylight-saving rule for the hour, turn on these and not on which
    # values the field ends up holding.
    any_hour: bool
    any_day: bool
    any_weekday: bool

    @classmethod
    def parse(cls, expression: str) -> Cron:
        """The expression, or a ValueError that says which field is wrong."""
        fields = _ALIASES.get(expression.strip().lower(), expression).split()
        if len(fields) != len(_FIELDS):
            raise ValueError(
                f"a cron expression has 5 fields (minute hour day month weekday); "
                f"{expression!r} has {len(fields)}"
            )
        minutes, hours, days, months, weekdays = (
            _field(text, *spec) for text, spec in zip(fields, _FIELDS, strict=True)
        )
        cron = cls(
            expression=" ".join(fields),
            minutes=tuple(sorted(minutes)),
            hours=tuple(sorted(hours)),
            days=days,
            months=months,
            weekdays=frozenset(day % 7 for day in weekdays),
            any_hour=fields[1].startswith("*"),
            any_day=fields[2].startswith("*"),
            any_weekday=fields[4].startswith("*"),
        )
        # `0 0 31 2 *` parses and never fires. Say so now, not at the first tick.
        cron.after(datetime(2000, 1, 1, tzinfo=UTC))
        return cron

    def _matches(self, day: date) -> bool:
        if day.month not in self.months:
            return False
        by_date = day.day in self.days
        by_weekday = day.isoweekday() % 7 in self.weekdays
        if self.any_day or self.any_weekday:
            return by_date and by_weekday
        return by_date or by_weekday

    def after(self, moment: datetime, tz: tzinfo = UTC) -> datetime:
        """The first fire time strictly after `moment`, reading the expression as wall-clock
        time in `tz`. Returned in UTC."""
        if moment.tzinfo is None:
            raise ValueError("moment must be timezone-aware")
        # Compared in UTC throughout: two datetimes that share a tzinfo are compared by
        # their wall-clock fields, which is wrong in exactly the hours this has to get right.
        moment = moment.astimezone(UTC)
        day = moment.astimezone(tz).date()
        for _ in range(_HORIZON_DAYS):
            if day.month not in self.months:
                day = (day.replace(day=1) + timedelta(days=32)).replace(day=1)
                continue
            if self._matches(day) and (found := self._on(day, moment, tz)) is not None:
                return found
            day += _DAY
        raise ValueError(f"{self.expression!r} never fires")

    def _on(self, day: date, moment: datetime, tz: tzinfo) -> datetime | None:
        """The first fire time on `day` (a date in `tz`) that is after `moment`."""
        midnight = datetime(day.year, day.month, day.day)
        ordinary = (
            midnight.replace(tzinfo=tz).utcoffset()
            == (midnight + _DAY).replace(tzinfo=tz).utcoffset()
        )
        best: datetime | None = None
        for hour in self.hours:
            for minute in self.minutes:
                wall = midnight.replace(hour=hour, minute=minute)
                # fold=0, the default, reads a skipped time with the offset from before the
                # change, which is the instant it would have been without the change.
                first = wall.replace(tzinfo=tz)
                instants = [first]
                if not ordinary and self.any_hour:
                    second = wall.replace(tzinfo=tz, fold=1)
                    # A smaller offset the second time round: the clocks went back, and
                    # this wall-clock time happens twice.
                    if second.utcoffset() < first.utcoffset():  # type: ignore[operator]
                        instants.append(second)
                for instant in instants:
                    instant = instant.astimezone(UTC)
                    if instant > moment and (best is None or instant < best):
                        best = instant
                # On a day without a change, wall-clock order is time order: the first
                # match is the answer. On a day with one it is not, so look at them all.
                if ordinary and best is not None:
                    return best
        return best
