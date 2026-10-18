# Contributing

Thanks for looking. Bug reports, fixes, documentation corrections and benchmark results
from other hardware are all welcome.

## Before you write code

For anything larger than a fix, open an issue first and say what you want to change and
why. The project has a deliberately narrow scope (a task queue whose only infrastructure is
PostgreSQL; the API's optional Redis holds request rate-limit counters and nothing else), and it is quicker to agree on direction before the work than after.

Things that are out of scope, so you do not spend time on them: a second storage backend,
a broker, a workflow or DAG engine, a JavaScript build step or framework for the web UI.

Things that would be very welcome:

- Results from `python -m benchmarks all` on a Linux server, with the hardware described.
- Testing on PostgreSQL versions other than 18, and behind PgBouncer. Both are marked
  "not tested" in the docs today.
- Kubernetes and systemd deployment examples that you have actually run.
- A client or worker for another language, built on the
  [HTTP worker protocol](docs/api.md#remote-workers).

## Setting up

You need [uv](https://docs.astral.sh/uv/) and Docker.

```bash
git clone https://github.com/mamedshahmaliyev/pgtaskqueue.git
cd pgtaskqueue
uv sync --all-extras
cp .env.example .env            # set TQ_JWT_SECRET to run the API; the tests do not need it
docker compose up -d db         # PostgreSQL on localhost:54320
uv run pytest -q
```

The tests create and use a database of their own (`task_queue_test`) and take about 40
seconds.

## Making a change

[AGENTS.md](AGENTS.md) is the guide to the code base: the layout, the rules that keep the
queue correct, how to add a setting, an endpoint, a migration. Read the "Rules that must
keep holding" section before touching `queue.py` or `worker.py`.

Before you open a pull request:

```bash
uv run ruff format .
uv run ruff check .
uv run pytest -q
```

A pull request should:

- **Do one thing.** A fix and an unrelated refactor are two pull requests.
- **Come with a test** that fails without the change. For anything concurrent, test the
  race, and check that the test fails when the protection is removed.
- **Update the docs** it makes untrue. Each topic has one page in [docs/](docs/); the table
  in AGENTS.md says which.
- **State only what you ran.** If you add a command or a number to the docs, you have run
  it. If something is untested, the text says so.
- **Include a migration** if it changes the schema, written to be safe on a live queue
  ([AGENTS.md](AGENTS.md#migrations)).

Commit messages: a short imperative summary line, then why the change is needed if that is
not obvious from the diff.

## Reporting a bug

Open an issue with:

- What you did, what you expected, what happened.
- The versions: `tq --version`, Python, PostgreSQL (`SELECT version()`).
- The task row if one is involved (`tq show <id> --logs`), with anything private removed.
- Worker logs around the time, at `--log-level DEBUG` if you can reproduce it.

For a security problem, do not open an issue. See [SECURITY.md](SECURITY.md).

## License

By contributing you agree that your contribution is licensed under the
[MIT License](LICENSE), like the rest of the project.
