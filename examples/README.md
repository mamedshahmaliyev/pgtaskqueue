# Examples

Runnable code for each part of the system. The executors here are what the compose
project's two workers run.

| File | Shows |
|---|---|
| [executors/basic.py](executors/basic.py) | One small task per feature: a function task, progress, retries, `Retry` / `Snooze` / `NonRetryable`, captured library logs, a synchronous executor run in a thread |
| [executors/pipeline.py](executors/pipeline.py) | A task made of durable steps that resumes where it failed; a task that starts other tasks |
| [executors/periodic.py](executors/periodic.py) | Tasks that run on a schedule: an interval, a cron expression in a timezone, two schedules for one task. The compose workers do not load it. |
| [executors/image.py](executors/image.py) | Tasks with a dependency of their own (Pillow), for a worker built from its own image |
| [workers/image/requirements.txt](workers/image/requirements.txt) | What that worker installs and the others do not |
| [enqueue.py](enqueue.py) | Enqueueing from Python, straight to the database: priorities, delays, deduplication, batches, enqueueing inside your own transaction |
| [api_client.py](api_client.py) | The HTTP API: sign in, list task types, enqueue, wait for the result, read the log |
| [webhook_receiver.py](webhook_receiver.py) | Receiving a task's callback and verifying its signature, with the standard library only |
| [remote_worker.py](remote_worker.py) | A worker that never touches the database: the whole HTTP worker protocol in one file |

## Running them

With the compose project up (`docker compose up -d --build`, see the
[README](../README.md#quick-start)), the task examples need nothing else:

```bash
docker compose exec worker tq enqueue demo.echo '{"message": "hello"}' --wait
docker compose exec worker tq enqueue demo.flaky '{"fail_times": 2}' --wait
docker compose exec worker tq enqueue report.build '{"rows": 500, "fail_once_at": "transform"}' --wait
docker compose exec worker tq enqueue image.render '{"width": 1600, "height": 900}' --wait
```

The scripts run on your machine, against the compose project. They need the development
environment (`uv sync --all-extras`) and the `.env` you created for compose:

```bash
uv run python -m examples.enqueue

TQ_PASSWORD='<your admin password>' uv run python -m examples.api_client
```

### Webhooks

In one terminal, start the receiver:

```bash
uv run python -m examples.webhook_receiver          # listens on :9000; PORT=… to change
```

The compose worker reaches your machine as `host.docker.internal`, which is a private
address, so the worker must be allowed to call it: set `TQ_WEBHOOK_ALLOW_PRIVATE=true` in
`.env` and run `docker compose up -d worker`. Then enqueue a task with a callback:

```bash
curl -s -X POST http://localhost:8010/v1/tasks -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{
    "name": "demo.echo", "payload": {"message": "hi"},
    "callback": {"url": "http://host.docker.internal:9000/hooks/tq", "secret": "whsec-example-secret"}
  }'
```

The receiver prints `task.succeeded: demo.echo <id> → {...}`. [docs/api.md](../docs/api.md#webhooks)
describes the request and the signature.

### A remote worker

```bash
docker compose exec api tq user create gpu-1 --service --role worker --task-names 'remote.*' --generate

TQ_API_URL=http://localhost:8010 TQ_CLIENT_ID=gpu-1 TQ_CLIENT_SECRET='<the client secret printed above>' \
  uv run python -m examples.remote_worker
```

and from another terminal:

```bash
docker compose exec worker tq enqueue remote.shout '{"text": "hello"}' --wait
```

## Running a worker without Docker

```bash
uv run tq migrate
uv run tq worker --executors examples.executors.basic,examples.executors.pipeline

# the image worker, with its own dependency:
uv run --with-requirements examples/workers/image/requirements.txt \
  tq worker --executors examples.executors.image --tasks 'image.*:2'
```
