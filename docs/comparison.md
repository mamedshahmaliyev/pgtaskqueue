# Why a queue in PostgreSQL, and when not

This project keeps the queue in the database you already run. That is a trade, not a free
win. This page says what you get, what you give up, and when another tool is the better
choice.

- [The short version](#the-short-version)
- [What you get](#what-you-get)
- [What you give up](#what-you-give-up)
- [Against a broker-based queue](#against-a-broker-based-queue)
- [Against other PostgreSQL queues](#against-other-postgresql-queues)
- [Against workflow engines](#against-workflow-engines)
- [What this project does not have](#what-this-project-does-not-have)
- [Choosing](#choosing)

## The short version

Use this when you want to **see and control** background work: who enqueued what, what it
is doing, why it failed, how long each kind takes, all in SQL, next to your data, with
nothing new to operate.

Use a broker (Redis, RabbitMQ, SQS, Kafka) when you need to move tens of thousands of small
messages a second, or to fan messages out across many services, and you can afford to know
less about each one.

## What you get

**The queue is a table you can query.** Every task is a row that stays: payload, status,
attempts, timings, output, error, traceback, the worker that ran it, the account that
enqueued it. Every log line is a row. "Which tenant's reports got slower this week" is a
`GROUP BY`. With a broker, a message is gone once it is consumed, and answering that
question means building a second system to record what the first one did.
[analytics.md](analytics.md) is a page of such queries.

**Tasks commit with your data.** Enqueue in the same transaction as the business write, and
either both happen or neither does. No task for an order that rolled back; no order whose
task was lost because the broker was briefly unreachable. With a separate broker this takes
an outbox table and a relay process, which is a PostgreSQL queue you built yourself.

**Control while it runs.** Change a waiting task's priority, payload or schedule. Cancel a
running one and have it actually stop. Pause a queue. Requeue last night's failures with
one command once the cause is fixed. Find the task a customer is asking about by their id.
These are updates to rows, so they are simple and immediate.

**One thing to run.** No broker to size, secure, monitor, upgrade and back up. Your
existing PostgreSQL backups cover the queue, and a restore brings the queue back in a state
consistent with the rest of your data.

**Nothing is lost quietly.** A task is durable when `enqueue` returns. A worker that dies
has its tasks recovered by the others. A task that exhausts its attempts stays in the table
as `failed`, with its traceback, until you decide what to do with it. There is no separate
dead-letter queue to remember to look at.

**Access control and an API come with it.** Roles, per-account limits on queues and task
names, per-account quotas (tasks waiting, tasks running, tasks a minute), JWT sign-in,
an HTTP API, webhooks, and an MCP server for AI assistants. With most queue libraries, exposing the queue safely to other teams or services is yours to build.

## What you give up

**Raw throughput.** One PostgreSQL moved about 4,000 no-op tasks a second end to end in
[our benchmark](benchmarks.md), on a laptop. Redis-backed queues are commonly reported an
order of magnitude above that. Each task here costs a few durable database round trips,
and that is the price of everything in the section above. If your tasks take 50 ms or more
to run, you will not notice. If they take 50 microseconds, you will.

**Load on your main database.** The queue's writes share your PostgreSQL's disk, memory,
connections and vacuum with your application. For most systems this is small next to the
application's own traffic. At high volume it is not, and then the queue wants a database of
its own, at which point you have something to operate after all.

**Fan-out and routing.** A broker can deliver one message to many independent consumers,
route by topic, and replay a stream. This is a job queue: one task, one executor, one
outcome.

**An ecosystem.** Celery has more than fifteen years of integrations, documentation, hosted
dashboards and people who have hit your problem before. This project is new.

**Other languages.** Workers here are Python. Other languages can take part through the
[HTTP worker protocol](api.md#remote-workers), which is small, but there is no native
client for them.

## Against a broker-based queue

Celery, RQ, Dramatiq, Arq, Huey and similar, on Redis or RabbitMQ.

| | This project | Broker-based queue |
|---|---|---|
| Where tasks live | Rows in PostgreSQL, kept | Messages in a broker, gone when consumed |
| History and results | The same rows, as long as you like | A separate result backend, usually with expiry |
| "What is queued, by tenant?" | A SQL query | Not generally answerable without extra bookkeeping |
| Enqueue with your transaction | Yes | No; needs an outbox |
| Change or cancel a waiting task | Yes, by id | Limited; often only "revoke by id" checked at pickup |
| Per-task logs | Stored with the task | Wherever your log pipeline puts them |
| Throughput ceiling | Thousands a second per database | Tens of thousands and up |
| Extra infrastructure | None | A broker, often a result store too |
| Concurrency model | asyncio; thousands of waiting tasks per process | Varies: processes, threads, greenlets, asyncio |
| Maturity and ecosystem | New | Established |

The table generalizes over several different tools; check the one you are considering.
Where a broker-based queue is clearly better: very high message rates, tasks so small that
queue overhead dominates, and organisations that already run and understand the broker.

## Against other PostgreSQL queues

The technique at the core of this project, `SELECT … FOR UPDATE SKIP LOCKED`, is well known
and there are good libraries built on it. In Python, look at
[Procrastinate](https://procrastinate.readthedocs.io/) and
[PGQueuer](https://pgqueuer.readthedocs.io/); outside Python, River (Go), Oban (Elixir),
GoodJob and Solid Queue (Ruby), and the `pgmq` extension. If what you want is a library to
call from one application, compare them: some are more mature than this one, and several
have features this one lacks.

What this project adds on top of the queue itself:

- **A service, not only a library.** An HTTP API with OAuth2 sign-in, roles and
  per-account ACLs and quotas, so that other teams, services and languages can use one queue
  without database credentials, and one of them cannot crowd out the others.
- **Workers that are deliberately separate.** Each kind of work has its own image and its
  own dependencies. The API never imports task code; workers publish their task names and
  payload schemas to the database. A protocol for workers that cannot reach the database
  at all.
- **Operational detail as data.** Per-task logs, named steps whose results survive a
  retry, progress, worker heartbeats and counters, all queryable.
- **An MCP server**, so that an AI assistant can be given exactly as much of the queue as a
  role allows.
- **Measured behaviour.** [Benchmarks](benchmarks.md) you can re-run, with their limits
  stated.

## Against workflow engines

Temporal, Airflow, Prefect, Dagster and similar.

Those model *workflows*: graphs of steps with dependencies, schedules, backfills, and
(Temporal) durable execution of arbitrary code. They are larger systems, with their own
servers and concepts, and they are the right tool when the workflow is the product.

This project models *tasks*. It has two modest tools for multi-step work: a task can be
divided into [steps](executors.md#steps-resuming-instead-of-repeating) whose results are
saved, so that a retry resumes where it failed; and a task can
[enqueue other tasks](executors.md#starting-other-tasks). There is no graph, no "run B
after A and C succeed", no backfill. If you find yourself building those on top, use a
workflow engine.

## What this project does not have

Stated plainly, so you do not have to discover it:

- **No backfill and no overlapping runs for periodic tasks.** A
  [schedule](schedules.md) that missed fire times enqueues one task and carries on, and it
  has one active task at a time. Cron expressions have five fields: no seconds, no `L`,
  `W`, `#` or `?`. There is no jitter and no "run now".
- **No rate limits on running.** Concurrency per task name is limited by slots, and an
  account's tasks by its [running limit](auth.md#limits). An account's enqueues can be
  limited per minute, through the API only. "Run at most 100 of these an hour" is not
  expressible.
- **No workflow primitives.** No chains, groups or chords.
- **Archived tasks are out of reach of the API and the UI.** Once a finished task has been
  moved to the [archive](analytics.md#the-archive), only `tq archive` and SQL see it, and
  it cannot be requeued.
- **No exactly-once.** Delivery is at-least-once, as with every queue that survives
  crashes. Executors must tolerate running twice
  ([architecture.md](architecture.md#what-is-guaranteed)).
- **A small web UI, not a monitoring product.** [It](ui.md) shows tasks, queues, workers and
  statistics and it polls. No alerting, no long-term metrics, no schedule
  management.
- **No native clients outside Python.** There is HTTP.
- **No track record.** It has tests and benchmarks, and it is version 0.1.

## Choosing

| If | Then |
|---|---|
| You run PostgreSQL, and want to see, query and control background work | This project |
| Tasks must be created atomically with database writes | This project, or any PostgreSQL queue |
| Several teams or services need one queue with access control | This project |
| You want only a library inside one Python app | Look at Procrastinate first |
| Tens of thousands of tiny tasks a second | A broker-based queue |
| One event must reach many independent consumers | A message broker or a log (RabbitMQ, Kafka, SNS/SQS) |
| The work is a graph of dependent steps with schedules and backfills | A workflow engine |
| You have no PostgreSQL and do not want one | Not this |
