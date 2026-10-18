# Periodic tasks

A schedule enqueues a task on a cron expression or at a fixed interval. It is a row in
`tq_schedules`. There is no scheduler process: every worker looks for due rows once a
second, and whichever gets to a row first enqueues its task.

- [Declaring a schedule in code](#declaring-a-schedule-in-code)
- [Creating one at run time](#creating-one-at-run-time)
- [When it fires](#when-it-fires)
- [Cron expressions](#cron-expressions)
- [Timezones and daylight saving](#timezones-and-daylight-saving)
- [What the task looks like](#what-the-task-looks-like)
- [Pausing, changing and removing](#pausing-changing-and-removing)
- [How it works](#how-it-works)
- [Limits](#limits)

A runnable version of the examples is
[examples/executors/periodic.py](../examples/executors/periodic.py).

## Declaring a schedule in code

Next to the task it runs:

```python
from pgtaskqueue import Schedule, TaskContext, task


@task("demo.tick", schedule=Schedule(every=10))
async def tick(ctx: TaskContext, payload: dict) -> dict:
    return {"scheduled_for": ctx.metadata["scheduled_for"]}
```

On a class, `schedules` is a tuple, and a task can have several:

```python
from datetime import timedelta

from pgtaskqueue import Executor, Schedule, TaskContext, register


@register
class Digest(Executor[DigestPayload]):
    name = "demo.digest"
    payload_model = DigestPayload
    schedules = (
        Schedule(
            "0 8 * * mon-fri",
            timezone="Europe/Berlin",
            name="demo.digest.weekday",
            payload={"audience": "team"},
        ),
        Schedule(every=timedelta(hours=6), name="demo.digest.everyone", priority=200),
    )

    async def run(self, ctx: TaskContext, payload: DigestPayload) -> dict: ...
```

A worker writes the schedules of the tasks it runs to the database when it starts. Start
one, and they are there:

```bash
tq worker --executors examples.executors.periodic
tq schedule list
```

```
demo.digest.everyone         default/demo.digest  every 21600s  next 2026-10-13 21:59:47  code
demo.digest.weekday          default/demo.digest  0 8 * * mon-fri (Europe/Berlin)  next 2026-10-14 10:00:00  code
demo.tick                    default/demo.tick  every 10s  next 2026-10-13 16:00:57  code
```

`tq schedule list` prints times in the local zone of the machine it runs on; the API
returns UTC.

### `Schedule`

| Argument | Default | |
|---|---|---|
| `cron` (positional) | | A cron expression. Give this or `every`, not both. |
| `every` | | Seconds, or a `timedelta`. At least 1 second. |
| `timezone` | `"UTC"` | The IANA zone the cron expression's clock is read in. Not used by `every`. |
| `name` | the task name | The schedule's name, unique among all schedules. A task with two schedules must name at least one. |
| `payload` | `{}` | The payload of each task. Checked against the task's payload model when the worker starts. |
| `queue` | the worker's queue | The queue the task is put on. |
| `priority`, `max_attempts`, `timeout` | the task's own | As for [enqueue](executors.md). |

A wrong expression, an unknown timezone, a payload that does not match the model, or two
schedules with one name stop the worker at start with a message that says which.

## Creating one at run time

A schedule can also be created for any task name, without touching the task's code. These
three do the same thing.

```bash
tq schedule set nightly-echo demo.echo '{"message": "good night"}' \
    --cron '30 2 * * *' --timezone Europe/Berlin
```

```
created  nightly-echo                 default/demo.echo  30 2 * * * (Europe/Berlin)  next 2026-10-14 04:30:00  api
```

```bash
curl -X PUT "$API/v1/schedules/nightly-echo" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"task": "demo.echo", "payload": {"message": "good night"},
       "cron": "30 2 * * *", "timezone": "Europe/Berlin"}'
```

```python
from pgtaskqueue import Schedule, Schedules
from pgtaskqueue.db import Database

db = Database()
row = await Schedules(db).put(
    "nightly-echo",
    "demo.echo",
    Schedule("30 2 * * *", timezone="Europe/Berlin", payload={"message": "good night"}),
)
print(row.created, row.next_run_at)
```

The MCP server has the same as the `set_schedule` tool ([mcp.md](mcp.md)).

Setting a name that exists **replaces that schedule whole**: what is left out goes back to
its default. The pause state is kept. The next fire time is kept too, unless the cron
expression, the interval or the timezone changed.

Each schedule has a `source`: `code` for one a worker published, `api` for one created in
any of the three ways above. Neither overwrites the other:

- Setting a name that a `code` schedule holds is refused (HTTP 409, exit status 1 from
  `tq`). Change it in the code, or pause it.
- A worker that declares a name an `api` schedule holds leaves that one alone and logs a
  warning.

The API checks the payload against the task's published schema, as it does for enqueue.
`tq schedule set` and `Schedules.put` do not: they do not know the schema. A schedule for a
task name that no worker runs is accepted, and its tasks wait.

## When it fires

- **An interval** first fires one interval after the schedule is created, then keeps that
  phase: a schedule that fired at 12:00:47.6 with `every=10` is next due at 12:00:57.6,
  however late the previous fire was noticed.
- **A cron expression** fires at the next time that matches.
- A fire is noticed up to `TQ_SCHEDULE_INTERVAL_SECONDS` (1 second) late. The task is then
  claimed like any other, so when it starts depends on the queue.
- **Missed fire times collapse into one.** If no worker was running for three hours, or the
  schedule was paused, a schedule that is due enqueues one task when a worker next looks,
  and continues from then. Nothing is backfilled.
- **One active task per schedule.** If the previous task of a schedule is still waiting,
  running, or waiting to be retried when the next fire time comes, that fire is skipped
  and the worker logs it. A slow task gets a late run, not a backlog. To run copies of a
  task side by side, give it two schedules.
- If no worker is running, nothing fires. The API does not fire schedules.
- A schedule on a [paused queue](workers.md#queues) still enqueues its one task, which
  waits with the rest.

Delivery of the task is at-least-once, as for every task. A scheduled task can run twice
if its worker dies at the wrong moment, so write it to tolerate that.

## Cron expressions

Five fields: minute, hour, day of month, month, day of week.

| Form | Example | Means |
|---|---|---|
| A value | `30 2 * * *` | 02:30 every day |
| A list | `0 8,12,18 * * *` | 08:00, 12:00 and 18:00 |
| A range | `0 9-17 * * mon-fri` | On the hour, 09:00 to 17:00, Monday to Friday |
| A step | `*/15 * * * *` | Every 15 minutes |
| A step from a value | `5/20 * * * *` | At :05, :25 and :45 |
| Names | `0 0 1 jan,jul *` | Midnight on 1 January and 1 July |
| An alias | `@hourly` | `0 * * * *` |

Day of week is 0 to 7, where 0 and 7 are both Sunday, or `sun` to `sat`. Months are 1 to
12 or `jan` to `dec`. The aliases are `@yearly` (`@annually`), `@monthly`, `@weekly`,
`@daily` (`@midnight`) and `@hourly`.

When day of month **and** day of week are both restricted, a day matches if **either**
does, as in cron: `0 0 13 * fri` is every 13th and every Friday.

An expression that can never fire, such as `0 0 31 2 *`, is refused when the schedule is
created.

Not supported: a seconds field, `@reboot`, and the `L`, `W`, `#` and `?` extensions. For
anything more often than once a minute, use `every`.

## Timezones and daylight saving

A cron expression is read as clock time in the schedule's `timezone`, which is UTC unless
you say otherwise. Two rules cover the days the clocks change:

- A time that is skipped fires at the instant it would have: `30 2 * * *` in
  `Europe/Berlin` fires at 03:30 on the last Sunday of March.
- A time that happens twice fires the first time only. The exception is an hour field that
  starts with `*`: `*/15 * * * *` means "every so often", and fires through both hours.

Intervals are not affected: `every=3600` is every 3600 seconds.

Zones come from Python's `zoneinfo`, which reads the operating system's timezone database
or the `tzdata` package. `UTC` needs neither. The `python:3.13-slim` image the Dockerfile
builds from, and macOS, both resolved `Europe/Berlin` when tried. On Windows, or on an
image without the database, add `tzdata` to the worker's requirements; that has not been
tested.

A zone must be known to the process that creates the schedule and to the workers. A worker
that cannot read a schedule's zone logs a warning and leaves the row for another.

## What the task looks like

An ordinary task, with three things set:

```bash
tq tasks --name demo.tick --limit 1 --json
```

```json
{
  "id": "01a13522-fdbe-703d-bc05-406b77509b85",
  "queue": "default",
  "name": "demo.tick",
  "status": "succeeded",
  "metadata": {
    "schedule": "demo.tick",
    "scheduled_for": "2026-10-13T12:00:47.615319+00:00"
  },
  "dedupe_key": "tq.schedule:demo.tick",
  "created_by": null
}
```

(Other fields left out.)

- `metadata.schedule` is the schedule's name. `GET /v1/tasks?metadata={"schedule":"demo.tick"}`
  lists a schedule's runs.
- `metadata.scheduled_for` is the time the fire was due. Use it, not the clock, for "the
  period this run covers": the task may start late, be retried tomorrow, or run twice. In
  an executor it is `ctx.metadata["scheduled_for"]`.
- `dedupe_key` is what keeps the schedule to one active task.
- `created_by` is the account that created the schedule, `cli` for `tq schedule set`, and
  empty for a schedule declared in code.

A schedule row has `last_run_at` and `last_task_id`: the last time it enqueued a task, and
which.

In SQL, each schedule with its last run and how that run ended:

```sql
SELECT s.name, s.next_run_at, s.paused, s.last_run_at, t.status, t.spent_seconds
FROM tq_schedules s
LEFT JOIN tq_tasks t ON t.id = s.last_task_id
ORDER BY s.name;
```

## Pausing, changing and removing

```bash
tq schedule pause nightly-echo
tq schedule resume nightly-echo
tq schedule delete nightly-echo
```

The same over HTTP ([api.md](api.md#schedules)) and MCP.

- **Pause** stops a schedule firing. Tasks it already enqueued are not touched. When it is
  resumed, a fire time that passed in the meantime fires once.
- A paused `code` schedule stays paused when workers restart.
- **To change a `code` schedule**, change the code and deploy. Workers that start with the
  new code write it; the last one to start wins, as for task types.
- **To remove a `code` schedule**, remove it from the code. A worker deletes the `code`
  schedules of the task names it runs that are no longer declared. A schedule whose task
  no worker runs any more stays until you delete it.
- **Deleting a `code` schedule** by hand lasts until the next worker that declares it
  starts. To stop it for good, remove it from the code; to stop it for now, pause it.

Who may do what over the API is in [auth.md](auth.md#roles): `schedules:read` to list,
`schedules:manage` to change. An account's ACL covers a schedule through its queue and its
task name.

## How it works

Every worker runs a loop that, once a second, does this in one transaction:

```sql
SELECT … , now() AS now
FROM tq_schedules s
WHERE NOT s.paused AND s.next_run_at <= now()
ORDER BY s.next_run_at
LIMIT 100
FOR UPDATE SKIP LOCKED;

INSERT INTO tq_tasks … ON CONFLICT … DO NOTHING;    -- one per due schedule, with its dedupe key

UPDATE tq_schedules SET next_run_at = …, last_run_at = …, last_task_id = … WHERE name = …;
```

- The row lock is what makes a fire time fire once. A second worker's `SKIP LOCKED` steps
  over a row that is being fired, and by the time the lock is free the row is no longer
  due. No advisory lock and no leader.
- The task, its `NOTIFY` and the new `next_run_at` commit together. A worker that dies in
  the middle leaves the schedule still due, for the next worker.
- The next fire time is computed from the `now()` that query returned. The host's clock is
  not consulted.
- Any worker fires any schedule, whatever queue and tasks it serves, because firing only
  inserts a row. The worker that runs the task can be a different one.

## Limits

- No backfill, and no "run this schedule now". To run the task once out of turn, enqueue
  it.
- No overlap: one active task per schedule.
- Cron has minute resolution. Intervals go down to 1 second, and fire up to
  `TQ_SCHEDULE_INTERVAL_SECONDS` late.
- No seconds field and no `L`, `W`, `#`, `?` in cron.
- No jitter. Schedules with the same expression fire in the same second.
- A schedule that fires faster than its task finishes skips fires; it does not queue them.
- Not measured: how many schedules one database handles. A fire is one small transaction,
  and a loop takes 100 due schedules at a time.
