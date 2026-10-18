# Security

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Report it privately through GitHub: on the repository page, **Security → Report a
vulnerability**. Include what you found, how to reproduce it, and what an attacker could do
with it.

This is a volunteer-maintained project, so there is no guaranteed response time. Fixes are
released as a new version, with the reporter credited unless they prefer otherwise.

Only the latest release is supported. This project is at version 0.x and has not had an
independent security review.

## What the project protects, and what it assumes

Knowing where the boundaries are is most of using this safely.

**Whoever can connect to the database controls the queue.** The `tq` command and the
`TaskQueue` class talk to PostgreSQL directly and perform no authorization of their own.
Roles and ACLs exist only in the HTTP API. Database credentials are administrator
credentials.

**Executors are trusted code; payloads are not.** A worker runs the executor code you
deploy, with that process's privileges. A task's payload comes from whoever was allowed to
enqueue it, and should be validated like any other input: declare a Pydantic model for it,
and do not pass payload fields to a shell, a file path or a SQL string unchecked.

**The API authorizes every request** with a signed JWT, a role, and an ACL of queue and
task-name patterns. A task the caller may not see answers 404, not 403. Tracebacks are
returned only to roles that may read every task.

**The API cannot run code.** It never imports executor modules. Compromising the API gives
an attacker the queue (they can enqueue, cancel, and read what its database role can read),
not code execution on workers, unless an executor can be driven to it through its payload.

## Known limits

Read these before exposing the API beyond a trusted network.

- **Access tokens cannot be revoked before they expire.** They are verified by signature
  alone. Disabling an account or changing its password takes effect at the next refresh,
  so within `TQ_ACCESS_TOKEN_MINUTES` (15 by default). Tokens minted with `tq token` live
  as long as you asked for. Rotating the signing key invalidates all tokens at once.
- **Sign-ins are rate-limited only when Redis is configured.** With `TQ_REDIS_URL` set
  (compose sets it), attempts are limited per client address and failures per account
  name ([auth.md](docs/auth.md#request-rate-limits)), and refused attempts cost no
  Argon2. There is no lockout, only throttling. Without Redis, or while it is down,
  nothing limits sign-ins: put a rate limit on `/v1/auth/token` and `/ui/login` at your
  reverse proxy. Behind a proxy, set `FORWARDED_ALLOW_IPS`, or every client shares the
  proxy's address limit.
- **Webhook URLs are a server-side request forgery surface.** A caller who may enqueue
  chooses a URL that a worker will then request from inside your network. By default
  workers refuse URLs that resolve to loopback, private or link-local addresses, and do not
  follow redirects. The check resolves the name, and the connection that follows resolves
  it again, so it does not stop an attacker who controls DNS for the name and changes the
  answer in between. If
  untrusted parties can enqueue, send worker egress through a proxy that enforces the
  rule, and leave `TQ_WEBHOOK_ALLOW_PRIVATE` off.
- **Task data is stored in plain text.** Payloads, outputs, errors, log lines, and a
  callback's secret and custom headers are ordinary columns. Anyone who can read the
  database, or a backup of it, can read them. Do not put credentials in payloads; pass a
  reference and let the executor fetch the secret. The API never returns a callback's
  secret or header values, only whether they are set.
- **Logs and tracebacks may contain sensitive values.** Whatever an executor logs, or a
  library logs on its behalf, is stored with the task and shown to anyone who may read it.
- **A remote worker sees the payloads it claims.** An account with the `worker` role can
  claim any task its ACL covers and read its payload. Scope each remote worker's account
  to the task names it runs (`--task-names`).
- **An MCP client's model sees what its token can read.** Task payloads, outputs and logs
  returned by a tool call enter the assistant's context. Give assistants an account scoped
  to what they need; `viewer` with an ACL is a good default.
- **The MCP host check is off by default.** Set `TQ_MCP_ALLOWED_HOSTS` in production.
- **No request size limits beyond the payload.** `TQ_MAX_PAYLOAD_BYTES` caps a task's
  payload. Cap request bodies and connection counts at your reverse proxy as well.

## Deploying it safely

- Serve the API over TLS only. Tokens are bearer credentials.
- Generate `TQ_JWT_SECRET` (`openssl rand -hex 32`); never reuse one from an example.
  Prefer a key pair (`ES256`) if other services verify tokens.
- Do not publish the database port. The compose file binds it to `127.0.0.1`.
- Change the bootstrap admin password after first use, and create named accounts with the
  least role that works. Use `service` accounts with generated secrets for programs.
- Give the API and workers a database role that can read and write the `tq_` tables and
  nothing else, and a different role to migrations.
- Keep `.env` out of version control and out of images. The compose file passes each
  container only the variables it needs; workers do not receive the JWT secret.
- Workers run as an unprivileged user in the provided image. Keep it that way, and mount
  only what an executor needs.

[docs/auth.md](docs/auth.md#hardening-checklist) and
[docs/infrastructure.md](docs/infrastructure.md#production-checklist) have the full
checklists.
