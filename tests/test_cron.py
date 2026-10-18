"""Cron expressions: what they parse to and when they fire. No database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from pgtaskqueue.cron import Cron
from pgtaskqueue.schedules import next_run

BERLIN = ZoneInfo("Europe/Berlin")
# A Tuesday.
START = datetime(2026, 10, 13, 12, 0, 30, tzinfo=UTC)


def fires(expression: str, start: datetime = START, tz=UTC, count: int = 3) -> list[str]:
    cron, moment, found = Cron.parse(expression), start, []
    for _ in range(count):
        moment = cron.after(moment, tz)
        found.append(f"{moment.astimezone(tz):%a %Y-%m-%d %H:%M%z}")
    return found


def test_fields_lists_ranges_steps_and_names():
    cron = Cron.parse("5/20 8-10,22 1,15 jan,JUL mon-fri")
    assert cron.minutes == (5, 25, 45)
    assert cron.hours == (8, 9, 10, 22)
    assert (cron.days, cron.months) == ({1, 15}, {1, 7})
    assert cron.weekdays == {1, 2, 3, 4, 5}
    # Sunday is 0 and 7.
    assert Cron.parse("0 0 * * 7").weekdays == Cron.parse("0 0 * * sun").weekdays == {0}
    assert Cron.parse("  @daily ").expression == "0 0 * * *"


def test_next_fire_times():
    assert fires("*/15 * * * *") == [
        "Tue 2026-10-13 12:15+0000",
        "Tue 2026-10-13 12:30+0000",
        "Tue 2026-10-13 12:45+0000",
    ]
    assert fires("0 9 * * mon-fri", count=4)[-2:] == [
        "Fri 2026-10-16 09:00+0000",
        "Mon 2026-10-19 09:00+0000",
    ]
    assert fires("@monthly", count=2) == ["Sun 2026-11-01 00:00+0000", "Tue 2026-12-01 00:00+0000"]
    # Read as wall-clock time in the schedule's zone: 03:00 in Berlin is 01:00 UTC in summer.
    assert Cron.parse("0 3 * * *").after(START, BERLIN) == datetime(2026, 10, 14, 1, tzinfo=UTC)


def test_the_next_fire_is_strictly_after_the_moment():
    on_the_minute = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
    assert Cron.parse("* * * * *").after(on_the_minute) == on_the_minute + timedelta(minutes=1)


def test_day_of_month_and_day_of_week_are_either_or_when_both_are_given():
    # Every 13th and every Friday: cron's own rule.
    assert fires("0 0 13 * fri", count=6) == [
        "Fri 2026-10-16 00:00+0000",
        "Fri 2026-10-23 00:00+0000",
        "Fri 2026-10-30 00:00+0000",
        "Fri 2026-11-06 00:00+0000",
        "Fri 2026-11-13 00:00+0000",
        "Fri 2026-11-20 00:00+0000",
    ]
    assert "Sun 2026-12-13 00:00+0000" in fires("0 0 13 * fri", count=12)
    # With one of them a wildcard, the other alone decides.
    assert fires("0 0 13 * *", count=1) == ["Fri 2026-11-13 00:00+0000"]


def test_a_date_that_is_rare_is_found_and_one_that_never_comes_is_refused():
    assert fires("0 0 29 2 *", count=2) == [
        "Tue 2028-02-29 00:00+0000",
        "Sun 2032-02-29 00:00+0000",
    ]
    # 2100 is not a leap year: the wait from 2096 is eight years.
    late = datetime(2096, 3, 1, tzinfo=UTC)
    assert Cron.parse("0 0 29 2 *").after(late) == datetime(2104, 2, 29, tzinfo=UTC)
    with pytest.raises(ValueError, match="never fires"):
        Cron.parse("0 0 31 2 *")


def test_a_wall_clock_time_skipped_by_daylight_saving_still_fires():
    """Berlin, 28 March 2027: 02:00 becomes 03:00."""
    before = datetime(2027, 3, 27, 12, tzinfo=UTC)
    assert fires("30 2 * * *", before, BERLIN) == [
        "Sun 2027-03-28 03:30+0200",
        "Mon 2027-03-29 02:30+0200",
        "Tue 2027-03-30 02:30+0200",
    ]
    # An hourly schedule fires once per real hour, not twice at 03:00.
    assert fires("0 * * * *", datetime(2027, 3, 28, 0, 30, tzinfo=UTC), BERLIN) == [
        "Sun 2027-03-28 03:00+0200",
        "Sun 2027-03-28 04:00+0200",
        "Sun 2027-03-28 05:00+0200",
    ]


def test_a_wall_clock_time_that_happens_twice_fires_once_unless_the_hour_is_a_wildcard():
    """Berlin, 25 October 2026: 03:00 becomes 02:00."""
    before = datetime(2026, 10, 24, 23, 45, tzinfo=UTC)
    assert fires("30 2 * * *", before, BERLIN, count=2) == [
        "Sun 2026-10-25 02:30+0200",
        "Mon 2026-10-26 02:30+0100",
    ]
    assert fires("30 * * * *", before, BERLIN) == [
        "Sun 2026-10-25 02:30+0200",
        "Sun 2026-10-25 02:30+0100",
        "Sun 2026-10-25 03:30+0100",
    ]


@pytest.mark.parametrize(
    ("expression", "complaint"),
    [
        ("* * * *", "has 4"),
        ("0 0 * * * *", "has 6"),
        ("60 * * * *", "minute: '60' is outside 0-59"),
        ("* 24 * * *", "hour"),
        ("* * 0 * *", "day of month"),
        ("* * * 13 *", "month"),
        ("* * * * 8", "day of week"),
        ("*/0 * * * *", "step"),
        ("5-1 * * * *", "outside"),
        ("1,,2 * * * *", "is not a value"),
        ("* * * foo *", "'foo' is not a value"),
        ("@reboot", "has 1"),
        ("", "has 0"),
    ],
)
def test_expressions_that_are_refused_say_why(expression, complaint):
    with pytest.raises(ValueError, match=complaint):
        Cron.parse(expression)


def test_an_interval_keeps_its_phase_and_steps_over_what_it_missed():
    def at(seconds: float) -> datetime:
        return START + timedelta(seconds=seconds)

    # New: one interval from now.
    assert next_run(cron=None, every_seconds=60, after=START) == at(60)
    # Noticed 2s late: still on the minute it was due on, not 2s later each time round.
    assert next_run(cron=None, every_seconds=60, after=at(2), previous=START) == at(60)
    # Ten minutes late: the missed fire times are stepped over, not queued up.
    assert next_run(cron=None, every_seconds=60, after=at(615), previous=START) == at(660)
    assert next_run(cron=None, every_seconds=60, after=at(60), previous=START) == at(120)
