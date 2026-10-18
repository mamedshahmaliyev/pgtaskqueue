# Authentication and access control

Who may call the API, and what they may do. This applies to the HTTP API, the MCP
endpoint and the [web UI](ui.md), which signs in with the same accounts and keeps its
tokens in cookies ([how](ui.md#how-the-session-works)). The `tq` command and the Python `TaskQueue` class talk to PostgreSQL directly and
are governed by database credentials instead.

- [The model](#the-model)
- [Roles](#roles)
- [ACLs](#acls)
- [Accounts](#accounts)
- [Who may manage whom](#who-may-manage-whom)
- [Limits](#limits)
- [Request rate limits](#request-rate-limits)
- [Getting a token](#getting-a-token)
- [Scopes: a token with less than its account](#scopes-a-token-with-less-than-its-account)
- [Token lifetimes and revocation](#token-lifetimes-and-revocation)
- [Signing keys](#signing-keys)
- [Using your own identity provider](#using-your-own-identity-provider)
- [Hardening checklist](#hardening-checklist)

## The model

Two questions are asked of every request, and both must be answered yes:

1. **Role**: may this account do this *kind* of thing? (enqueue, cancel, pause a queue)
2. **ACL**: may it do it to *this* queue and task name?

A third applies to tasks: without the `_all` permissions, an account sees and manages only
the tasks **it created**.

The answer comes from the access token alone. Role and ACL are claims inside a signed JWT,
so verifying a request costs a signature check and no database query.

## Roles

| Role | For | Can |
|---|---|---|
| `admin` | Whoever runs the system | Everything, including accounts. |
| `manager` | A team lead who looks after a team's accounts | What an operator can, and manage accounts that reach no further than its own ([rules](#who-may-manage-whom)). |
| `operator` | Day-to-day operation, on-call | See and manage every task, pause queues, manage schedules, read statistics. No accounts. |
| `submitter` | An application that submits work | Enqueue, and see and manage **its own** tasks. |
| `viewer` | Dashboards, auditors, a read-only assistant | See every task, schedules, statistics and workers. Change nothing. |
| `worker` | A [remote worker](api.md#remote-workers) | Claim and settle tasks. Nothing else. |

The permissions behind them:

| Permission | admin | manager | operator | submitter | viewer | worker |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| `tasks:enqueue` | ✓ | ✓ | ✓ | ✓ | | |
| `tasks:read` (own tasks) | ✓ | ✓ | ✓ | ✓ | ✓ | |
| `tasks:read_all` (everyone's, with tracebacks) | ✓ | ✓ | ✓ | | ✓ | |
| `tasks:manage` (cancel, requeue, edit own) | ✓ | ✓ | ✓ | ✓ | | |
| `tasks:manage_all` | ✓ | ✓ | ✓ | | | |
| `tasks:work` (remote-worker protocol) | ✓ | | | | | ✓ |
| `queues:manage` (pause, resume) | ✓ | ✓ | ✓ | | | |
| `schedules:read` | ✓ | ✓ | ✓ | | ✓ | |
| `schedules:manage` (create, replace, pause, delete) | ✓ | ✓ | ✓ | | | |
| `stats:read` | ✓ | ✓ | ✓ | | ✓ | |
| `workers:read` | ✓ | ✓ | ✓ | | ✓ | |
| `users:manage` | ✓ | ✓ | | | | |

The set is fixed, in [auth/roles.py](../src/pgtaskqueue/auth/roles.py). To give an account
less than its role, use an ACL or a [scope](#scopes-a-token-with-less-than-its-account).

## ACLs

Every account has two lists of glob patterns: the **queues** and the **task names** it may
touch. Both default to `*`.

```bash
tq user create billing-svc --service --role submitter \
    --queues 'default' --task-names 'invoice.*,mail.send' --generate
```

That account can enqueue `invoice.render` and `mail.send` on the `default` queue, and
nothing else. Asking for anything outside its ACL is a 403 on enqueue, and tasks outside it
are invisible (404) when reading.

ACLs apply to every role except where noted:

- Listings (`/v1/tasks`, `/v1/stats`, `/v1/queues`, `/v1/workers`, `/v1/task-types`) are
  filtered to what the ACL allows.
- A `worker` account can claim only task names and queues in its ACL.
- An `admin` or `manager` with a narrowed ACL is narrowed for tasks, and manages only
  accounts whose ACL is inside its own ([below](#who-may-manage-whom)).

This is how one deployment serves several tenants or teams: name tasks with a prefix per
owner (`billing.*`, `search.*`) or give each a queue, and scope accounts to match.

## Accounts

Two kinds:

| Kind | Signs in with | For |
|---|---|---|
| `user` | Username and password | A person. Gets a refresh token. |
| `service` | Client id and secret | A program. Asks for a new token when it needs one. |

Manage them with the CLI (which needs database access), the **Accounts** page of the
[web UI](ui.md#accounts), or the API. The UI and the API need an account with
`users:manage`, an `admin` or a `manager`:

```bash
tq user create alice --role operator                    # prompts for a password
tq user create svc-reports --service --role submitter --generate   # prints a secret once
tq user set-password alice
tq user list
```

```bash
curl -s -X POST http://localhost:8010/v1/users -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username": "bob", "password": "a long passphrase", "role": "viewer"}'

curl -s -X PATCH http://localhost:8010/v1/users/bob -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"is_active": false}'
```

Passwords and client secrets are hashed with Argon2id and never stored or logged in the
clear. A generated secret is shown once. Passwords must be at least 8 characters; prefer a
generated one for services.

**The first admin** is created by `tq migrate` from `TQ_BOOTSTRAP_ADMIN_USERNAME` and
`TQ_BOOTSTRAP_ADMIN_PASSWORD`, only when no account exists yet. It is safe to leave those
set on every deploy. Changing the variable later does not change the password; use
`tq user set-password`.

An admin cannot disable, demote or delete its own account, so the last admin cannot lock
itself out by accident.

## Who may manage whom

`users:manage` lets an account manage others, but never past itself. Without that rule a
manager could create an admin and sign in as it.

- **Roles.** It may give a role only if it holds every permission that role grants. A
  `manager` may give `manager`, `operator`, `submitter` and `viewer`, but not `admin` or
  `worker`, because both grant `tasks:work`, which a manager does not have.
- **ACLs.** It may give only patterns that are in its own ACL, compared literally: an
  account with `billing.*` may give `billing.*`, not `billing.invoice.*`. An account
  with `*` may give anything.
- **[Limits](#limits).** It may give each limit only as tight as its own effective one,
  default included: at most its `max_waiting`, `max_running` and `enqueue_per_minute`, at
  least its `min_priority`. Where its own is a number, `0` (no limit) and an unset value
  that takes a default of `0` are looser, so it must give a number. Where its own is no
  limit, it may give anything. Otherwise a manager on 2 running tasks could create an
  account with no limit and use that one.
- **Existing accounts.** It may change, set the password of, or delete only an account it
  could have created: one whose role, ACL and limits pass the three checks above. Any other
  is a 403 with "ask an admin". `GET /v1/users` marks each account with `manageable`, and the UI
  shows no buttons for the others.
- **Its own account.** No account may change its own role, disable itself or delete
  itself. Only an account with every permission (an `admin` with `*`) may change its own
  ACL or limits; for anyone else a limit it could lift itself would limit nothing. Anyone
  may change their own password.

An `admin` with the ACL `*` passes every check, so none of this limits it. That includes
limits: an admin may give any, whatever its own.

A role or ACL change takes effect when the account's next access token is issued, as with
[revocation](#token-lifetimes-and-revocation); disabling an account or setting its
password ends its sessions at once.

## Limits

Each account can be held to four limits on the tasks **it enqueues**. They answer "one
client must not swamp the queue for the others".

| Limit | What happens past it |
|---|---|
| `max_waiting` | Enqueue is refused with **429** while the account has this many tasks `scheduled` or `pending`. A batch bigger than the limit itself is **413**. |
| `max_running` | Nothing is refused. The account's tasks beyond this many `pending` or `running` wait, **held**, and are released oldest first as its others finish. |
| `enqueue_per_minute` | Enqueue is refused with **429** and a `Retry-After` header once the account has created this many tasks in the last 60 seconds. A batch bigger than the limit is **413**. |
| `min_priority` | Asking for a priority below it (lower runs first) is **403**, on enqueue, on `PATCH` and on a schedule. A task that names no priority gets the queue's default or the floor, whichever is higher. |

Set them per account, in the UI, the API or the CLI:

```bash
tq user set-limits svc-reports --max-waiting 1000 --max-running 20 --enqueue-per-minute 600
tq user set-limits svc-reports --max-running default      # back to the server default
```

```bash
curl -s -X PATCH http://localhost:8010/v1/users/svc-reports -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"max_waiting": 1000, "min_priority": 50}'
```

An account's own value wins; an unset one (`null`, or `default` in the CLI) takes the
server's default from `TQ_ACCOUNT_MAX_WAITING`, `TQ_ACCOUNT_MAX_RUNNING`,
`TQ_ACCOUNT_ENQUEUE_PER_MINUTE` and `TQ_ACCOUNT_MIN_PRIORITY`
([configuration](configuration.md#account-limits)). `0` means no limit, and every
default is 0, so nothing is limited until you set something. An account from
[your own identity provider](#using-your-own-identity-provider) has no row here and gets
the defaults.

`GET /v1/auth/limits` tells any account its own limits and how close it is to them;
`GET /v1/users/{username}/limits` tells an account manager another's.

How they work, and what they do not do:

- **They are read from the database on each enqueue**, not carried in the token, so a
  change applies to the next request.
- **The waiting and rate checks happen in the API**, in the transaction that inserts the
  tasks, under a lock per account: two requests from one account cannot both take the
  last place. Accounts do not wait for each other, and an account with neither limit
  takes no lock. A program that calls `TaskQueue.enqueue` directly is not checked.
- **The running limit is applied by the queue itself**, so it holds however a task
  arrives, through the API, `TaskQueue` or a schedule. A task from an account with a
  running limit is written as `scheduled` and marked `capped` (the UI says *held*). Once a
  second, one worker moves as many of each account's due held tasks to `pending` as the
  account has room for (`pending` plus `running` below the limit), in the order they fell
  due. So a held task can wait up to `TQ_PROMOTE_INTERVAL_SECONDS` after a place frees, and
  `pending` counts toward the limit: an account at its limit does not keep a backlog of
  claimable work in front of others. That includes pending tasks in a paused queue; an
  account whose released tasks sit in one gets no more released until they run or are
  canceled.
  [Architecture](architecture.md#account-limits) has the details.
- **The running limit is the account's, not a queue's or a task name's.** For a limit on a
  kind of work, use [slots and pools](workers.md#slots-and-pools).
- **The rate limit counts tasks created**, not requests: a batch of 50 is 50.
- **Schedules are never refused.** A periodic task fires whatever the account's waiting or
  rate count; the tasks it makes count toward them, and toward the running limit, which
  holds them like any other.
- **Built-in `tq.` tasks** (webhook deliveries) count toward nothing.
- **Changing a running limit** applies to tasks enqueued afterwards. Tasks already held
  stay held until released; tasks enqueued before a limit was set are not held. Removing
  the limit releases held tasks at the next pass.

## Request rate limits

[Limits](#limits) govern the tasks an account makes. Request rate limits govern how fast
clients may call the API at all, sign-ins included. They are counted in Redis, so that
several API replicas share one count, and they apply only when `TQ_REDIS_URL` is set.
Compose runs a Redis and sets it.

| Limit | Counted by | Default |
|---|---|---|
| `TQ_RATE_LIMIT` | Account: the subject of a valid token, from the header or the UI's cookie | none |
| `TQ_RATE_LIMIT_ANONYMOUS` | Client address, for requests without a valid token | none |
| `TQ_LOGIN_RATE_LIMIT` | Client address: every password and client-credentials attempt, and every UI sign-in | `30/minute` |
| `TQ_LOGIN_FAILURE_LIMIT` | Account name: failed sign-ins only, from any address | `10/hour` |

```bash
TQ_REDIS_URL=redis://redis:6379/0
TQ_RATE_LIMIT=1200/minute
TQ_RATE_LIMIT_ANONYMOUS=120/minute
```

Past a limit the API answers **429** with `Retry-After` in seconds. The UI's login form
shows the wait instead. Requests that pass carry `RateLimit-Limit` and
`RateLimit-Remaining`.

How they work, and what they do not do:

- **`1200/minute` lets 1200 through at once, then one every 50 ms.** It is a token bucket
  (GCRA) that refills smoothly, not a counter that resets on the minute. A client that
  stops for a minute has its whole burst back.
- **The check and the count are one step**, a Lua script in Redis, so concurrent requests,
  on one replica or several, cannot pass more than the limit. Redis's clock is used, so
  replicas whose clocks disagree still agree.
- **A token is verified before its subject is counted.** A request with a made-up or
  expired token is counted by address, so inventing tokens does not escape the limit.
- **Sign-in limits are checked before the password is hashed.** A refused attempt costs no
  Argon2, which is what stops the token endpoint from being a way to burn the API's CPU.
  Refused attempts are refused even with the right password.
- **The failure limit stops a guesser who spreads over many addresses**, and it throttles
  rather than locks out: someone who knows an account name can delay its sign-ins by
  failing on purpose, but only while they keep failing, and an access token the account
  already holds keeps working. Two attempts in flight together can both pass the check, so
  a guesser can go over by its concurrency, not by a multiple.
- **The client address is what uvicorn reports.** Behind a reverse proxy, every request
  appears to come from the proxy and shares one address limit, unless uvicorn trusts the
  proxy's `X-Forwarded-For`: set `FORWARDED_ALLOW_IPS` to the proxy's address. Trust
  only your own proxy, or a client can name any address it likes.
- **Not counted**: `/health/*` and `/ui/static/*`. Everything else is, `/mcp` included.
- **If Redis is down**, requests go through unlimited, and a warning is logged at most
  every 5 seconds (`TQ_RATE_LIMIT_FAIL_OPEN=false` answers 503 instead). The enqueue
  [limits](#limits) still hold, because they live in PostgreSQL. A Redis call that takes
  more than half a second counts as down.
- **Redis holds counters and nothing else.** No task, account or token is stored there.
  Losing it resets the counters; nothing else is lost. Workers never use it.
- **One rate for every account.** There is no per-account override yet; for one client,
  the enqueue [limits](#limits) are per account.

## Getting a token

`POST /v1/auth/token` is an OAuth 2.0 token endpoint. The body is form-encoded.

**A person: the password grant**

```bash
curl -s http://localhost:8010/v1/auth/token \
  -d grant_type=password -d username=alice -d password='…'
```

```json
{"access_token": "eyJ…", "token_type": "bearer", "expires_in": 900, "refresh_token": "eyJ…"}
```

**A service: the client-credentials grant**

```bash
curl -s http://localhost:8010/v1/auth/token -u svc-reports:"$SECRET" -d grant_type=client_credentials
```

Credentials go in HTTP Basic auth, or as `client_id` and `client_secret` form fields. There
is no refresh token: the service asks again when the access token expires.

**Renewing: the refresh-token grant**

```bash
curl -s http://localhost:8010/v1/auth/token -d grant_type=refresh_token -d refresh_token="$REFRESH"
```

Returns a new access token and a new refresh token.

Then, on every request:

```
Authorization: Bearer eyJ…
```

`GET /v1/auth/me` shows what a token allows: its subject, role, permissions and ACL. The
"Authorize" button on `/docs` signs in with the password grant.

## Scopes: a token with less than its account

Add `scope` to a token request to get a token that can do only part of what its account
can. The value is a space-separated list of permission names:

```bash
curl -s http://localhost:8010/v1/auth/token \
  -d grant_type=password -d username=alice -d password='…' \
  -d scope='tasks:read tasks:read_all stats:read'
```

Alice is an operator, but this token can only read. Use it for a script, a dashboard, or an
AI assistant that should look and not touch.

A scope can only remove permissions. Asking for one the role does not have is ignored, not
an error; the `scope` field of the response says what was granted. A refreshed token keeps
the scope of the token it came from and can narrow it further, never widen it.

## Token lifetimes and revocation

| Token | Lifetime | Setting |
|---|---|---|
| Access | 15 minutes | `TQ_ACCESS_TOKEN_MINUTES` |
| Refresh | 7 days | `TQ_REFRESH_TOKEN_DAYS` |

An access token is verified without a database query. That is what keeps the API stateless,
and it has one consequence to understand: **an access token cannot be revoked before it
expires**. Changes take effect like this:

| Change | Access tokens already issued | Refresh tokens |
|---|---|---|
| Role or ACL changed | Keep the old role and ACL until they expire | New tokens get the new ones |
| Password changed | Valid until they expire | Revoked |
| Account disabled or deleted | Valid until they expire | Revoked |
| `POST /v1/auth/logout` | Valid until they expire | Revoked, for the whole account |

So the access-token lifetime is the longest a revoked credential keeps working. Fifteen
minutes is the default trade-off; lower it if that is too long for you.

Refresh tokens are checked against the database when used. They are not single-use: using
one does not invalidate it, and revocation is per account (everything at once), not per
token.

### Long-lived tokens for clients that cannot refresh

Some clients are configured with one static token: an MCP client, a dashboard, a cron job on
a machine you would rather not give a secret. Mint one from the command line:

```bash
tq token dashboard --days 30 --scope 'tasks:read tasks:read_all stats:read'
```

The token is printed alone on stdout, so `TOKEN=$(tq token …)` works. `tq token` needs the
database and the signing key, so it is an operator's command: run it where the API runs
(`docker compose exec api tq token …`).

Such a token is an access token with a long life, so it **cannot be revoked** before it
expires, short of rotating the signing key. Give it a dedicated account with the least role
that works, narrow it with `--scope`, and choose the shortest lifetime you can live with.

To cut off everything at once, including access tokens already issued, rotate the signing
key. Every token ever issued then fails verification.

## Signing keys

### One shared secret (default)

```bash
TQ_JWT_SECRET=$(openssl rand -hex 32)
```

HS256. The same secret signs and verifies, so anything that can verify tokens can also mint
them. Right when the API is the only thing that touches tokens. The secret must be at least
32 bytes; the API refuses to start otherwise.

### A key pair

```bash
openssl ecparam -name prime256v1 -genkey -noout -out tq-private.pem
openssl ec -in tq-private.pem -pubout -out tq-public.pem
```

```bash
TQ_JWT_ALGORITHM=ES256
TQ_JWT_PRIVATE_KEY=/run/secrets/tq-private.pem
TQ_JWT_PUBLIC_KEY=/run/secrets/tq-public.pem
```

The private key signs; the public key verifies. Other services can check a token with the
public key and cannot forge one. Any asymmetric algorithm PyJWT supports can be configured
(`ES256`/`384`/`512`, `RS256`/`384`/`512`, `PS256`/`384`/`512`, `EdDSA`); the test suite
covers `ES256`. Each key setting takes a file path or the PEM text itself
(with newlines written as `\n` if the variable must be one line). With only the private key
set, the public key is derived from it.

In every mode, verification is pinned to the one configured algorithm. A token's own `alg`
header never decides how it is checked, and `none` is refused.

## Using your own identity provider

Set an asymmetric algorithm and **only** the public key:

```bash
TQ_JWT_ALGORITHM=RS256
TQ_JWT_PUBLIC_KEY=/run/secrets/idp-public.pem
TQ_JWT_ISSUER=https://id.example.com/
TQ_JWT_AUDIENCE=pgtaskqueue-api
```

The API then verifies tokens and issues none: `POST /v1/auth/token` answers that tokens come
from elsewhere. Your provider must put these claims in its access tokens:

| Claim | Required | |
|---|---|---|
| `iss`, `aud` | yes | Must equal `TQ_JWT_ISSUER` and `TQ_JWT_AUDIENCE`. |
| `sub` | yes | Who is calling. Recorded as `created_by` on the tasks they enqueue. |
| `exp`, `iat` | yes | |
| `role` | yes | One of the five role names. A token without a known role is refused. |
| `queues`, `task_names` | no | The ACL: a list, or a comma-separated string. Default `*`. |
| `scope` | no | Space-separated permission names, to narrow the role. |

Most providers can add custom claims through a mapping rule or an "action". The key is read
from a PEM file at start-up; fetching and rotating keys from a JWKS URL is not implemented,
so a provider key rotation needs the file replaced and the API restarted.

## Hardening checklist

- **Serve the API over TLS**, behind a reverse proxy or load balancer. Tokens and passwords
  are bearer credentials; over plain HTTP anyone on the path can take them.
- **Generate `TQ_JWT_SECRET`** (`openssl rand -hex 32`). Do not reuse it elsewhere, and keep
  it out of the worker containers: workers do not need it.
- **Give each caller its own account**, with the least role and the narrowest ACL that
  works. A leaked `submitter` secret scoped to `invoice.*` is a small problem.
- **Use `service` accounts for programs**, with generated secrets.
- **Change the bootstrap admin password** after the first deploy, and create personal admin
  accounts instead of sharing it.
- **Keep the sign-in limits on**: set `TQ_REDIS_URL` (compose does), and keep
  `TQ_LOGIN_RATE_LIMIT` and `TQ_LOGIN_FAILURE_LIMIT` set. Without Redis the API does not
  rate-limit sign-ins, and each guess costs an Argon2 hash; limit `POST /v1/auth/token`
  and `POST /ui/login` at your proxy instead. Behind a proxy, set `FORWARDED_ALLOW_IPS`
  so the limits see client addresses
  ([how](#request-rate-limits)).
- **Set `TQ_UI_ENABLED=false`** if nobody uses the web UI. With it on, the API accepts the
  access token from a cookie as well as from the `Authorization` header.
- **Never run `tq seed`** against a database others can reach. Its accounts have a
  published password.
- **Set `TQ_CORS_ORIGINS`** only if a browser application calls the API, and only to that
  application's origin.
- **Restrict who can reach PostgreSQL.** Anyone with database credentials bypasses all of
  the above, by design: the `tq` command is an operator's tool.

[SECURITY.md](../SECURITY.md) covers the wider threat model, including webhooks and task
payloads.
