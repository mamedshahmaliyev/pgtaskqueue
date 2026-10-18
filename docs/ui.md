# Web UI

A few pages, served by the API at `/ui`, for watching the queue and acting on tasks: a
dashboard, the task list, one task with its log, and statistics. It signs in with the same
accounts as the API and can do what the account's [role and ACL](auth.md) allow.

- [Trying it](#trying-it)
- [The pages](#the-pages)
- [Cancel, kill and restart](#cancel-kill-and-restart)
- [What each role sees](#what-each-role-sees)
- [Demonstration data](#demonstration-data)
- [How the session works](#how-the-session-works)
- [How it is built](#how-it-is-built)
- [Limits](#limits)

## Trying it

The UI is part of `tq api`. Nothing else is started and nothing is built.

```bash
uv run tq migrate
uv run tq seed          # demonstration accounts and tasks; see below
uv run tq api           # http://127.0.0.1:8000/ui
```

Sign in as `demo-operator` with the password `demo-password`. To see the numbers move,
start a worker with the example executors in another terminal:

```bash
uv run tq worker --executors examples.executors.basic,examples.executors.pipeline
```

With the compose file the UI is at <http://localhost:8010/ui>, and you sign in with the
bootstrap admin account. `/` redirects to `/ui`.

Set `TQ_UI_ENABLED=false` to serve the API without it.

## The pages

| Page | Shows | Refreshes every |
|---|---|---|
| `/ui` | Counts of pending, running and scheduled tasks; what finished in the last hour, as a chart; queues, with a pause switch; workers; what is running; the latest failures | 5 seconds |
| `/ui/tasks` | Every task the account may see, newest first, with filters, the actions on a task, and the form that enqueues one | 5 seconds |
| `/ui/tasks/{id}` | One task: its fields, payload, output, progress, error and traceback, and its log, which is followed while the page is open | 2 seconds |
| `/ui/stats` | For a window of 15 minutes to 7 days: totals, finished tasks over time, and a table per queue and task name with failure rate, p50 and p95 run time, average wait and attempts | 10 seconds |
| `/ui/users` | Accounts, with role, ACL, limits and last sign-in, and the forms that create and change them. Only for an account with `users:manage` | 15 seconds |

A page stops refreshing while its tab is in the background.

**Filters.** The task list filters by status (any number of them), task name, queue, who
created the task, the worker that ran it, and metadata (a JSON object the task's metadata
must contain). The filters are kept in the address, so a filtered list can be bookmarked or
sent to someone, and the dashboard links into filtered lists. Name and queue match exactly;
there is no search by pattern.

**Enqueue.** The form takes what `POST /v1/tasks` takes, except a callback. Task names are
suggested from what workers have registered. Choosing one that published a payload schema
fills the payload with its fields. The API checks the payload against that schema and the
form shows what it answers.

**Bulk actions.** Tick tasks in the list to cancel, kill or restart them together. They are
sent one request at a time, and the page says how many worked and why the others did not.

## Cancel, kill and restart

The UI has three buttons for two API calls. In the task list each row shows them as icons;
hover over one for its name.

| Button | Shown for | Calls | Effect |
|---|---|---|---|
| **Cancel** | a `scheduled` or `pending` task | `POST /v1/tasks/{id}/cancel` | The task will not run. |
| **Kill** | a `running` task | the same | The task is marked `canceled` at once. Its worker interrupts it when it hears: immediately where `LISTEN`/`NOTIFY` is on, otherwise at its next heartbeat. |
| **Restart** | a `succeeded`, `failed` or `canceled` task | `POST /v1/tasks/{id}/requeue` | The same task runs again, attempts starting over. Completed steps are kept unless "from the first step" is ticked. |

Kill is cancel with a warning, because work is lost. The API call is the same; what differs
is that a worker holds the task and has to stop it.

- An `async` executor is interrupted at its next `await`.
- A plain `def` executor, and blocking code started with `ctx.to_thread`, runs in a thread.
  The worker raises `TaskStopped` in it, which arrives when the thread next runs Python
  code. A thread inside one long call into C (`time.sleep(600)`, a native library) stops
  when that call returns. See [executors.md](executors.md#stopping-a-thread).
- An `async` executor that computes without awaiting holds the event loop, so its worker
  hears nothing until it yields. Nothing stops that but ending the worker process.
- A task held by a [remote worker](api.md#remote-workers) stops when that worker's next
  heartbeat tells it the task is no longer its own.

A task that is running cannot be restarted in one step. Kill it, then restart it.

## What each role sees

The pages show what the API gives the account, so nothing is decided twice.

| Role | Dashboard and statistics | Tasks | Actions |
|---|---|---|---|
| `admin`, `manager` | Everything | All | As `operator`, and the Accounts page |
| `operator` | Everything | All | Enqueue, cancel, kill, restart, pause queues |
| `viewer` | Everything | All | None; the buttons are not shown |
| `submitter` | A note that the role has no statistics; its own running and failed tasks | Its own | Enqueue; cancel, kill and restart its own |
| `worker` | Nothing | None | None |

An ACL narrows all of it: an account limited to `report.*` sees those tasks, and statistics
for those names only. Service accounts cannot sign in to the UI; it takes a username and
a password.

The UI does not manage schedules. Use `tq schedule` or the API.

## Accounts

`/ui/users`, linked as **Accounts** in the header for an `admin` or a `manager`. It lists
every account, and for each: its role, its ACL, its four [limits](auth.md#limits) (an unset
one shows the server's default, marked "default"), and when it last signed in. A disabled
account is greyed.

- **New account** creates a person (a password) or a service (a client secret), with a
  role, an ACL and limits. The role list offers only roles this account may give.
- **Edit** changes role, ACL, limits, and whether the account is enabled. It shows what the
  account has now: tasks waiting and held, running, and enqueued in the last minute. An
  empty limit goes back to the default; `0` is no limit.
- **Password** (or **Secret**, for a service) sets a new one and ends the account's
  sessions.
- **Delete** removes the account. Its tasks stay.

A `manager` sees every account, but has buttons only for the ones within its own role and
ACL; the others say "beyond your role". On its own row it can set its password, and
nothing it could use to lift its own limits. The server makes each of these decisions
([rules](auth.md#who-may-manage-whom)); the page only leaves out the buttons that would be
refused.

A task held by its account's running limit is `scheduled`. Its page says that it is held,
and the dashboard's scheduled count says how many of them are.

## Demonstration data

`tq seed` fills an empty database so the pages have something to show.

```bash
tq seed [--tasks N] [--hours H] [--no-users] [--password P] [--force]
```

It writes:

- **Five accounts**: `demo-admin`, `demo-manager`, `demo-operator`, `demo-submitter`
  (limited to `demo.*` task names) and `demo-viewer`, all with the password `demo-password` unless `--password`
  gives another. An account that already exists is left as it is.
- **400 finished tasks** (`--tasks`) spread over the last 24 hours (`--hours`), more of them
  recent, with outcomes, run times and log lines. These are inserted as finished; no worker
  ran them.
- **Tasks in hand**: 12 pending, 5 scheduled, 3 running, and 6 pending on a queue named
  `bulk` that it pauses.

The task names and payloads are those of [examples/executors](../examples/executors), so a
worker started with them runs what is waiting. The three tasks seeded as `running` are held
by a worker that does not exist. A real worker's maintenance loop finds them stale after
`TQ_STALE_AFTER_SECONDS` and queues them again, like any task whose worker died.

`tq seed` refuses a database that already holds tasks, unless `--force`.

> The demo password is in this page. Do not seed a database that anyone else can reach,
> and delete the five accounts before a database is used for real work.

For a demo that strangers use, [demo.md](demo.md) has accounts with limits and a process
that resets it every hour.

## How the session works

A browser cannot be trusted to keep a token the way a service can: a token that JavaScript
can read is a token any injected script can send elsewhere. So the UI never gives its
tokens to the page.

1. `POST /ui/login` checks the username and password, as the password grant of
   `POST /v1/auth/token` does, and issues the same two tokens.
2. They are set as cookies: `tq_access` for the whole site, `tq_refresh` for `/ui` only.
   Both are `HttpOnly` and `SameSite=Lax`, and `Secure` when the request came over HTTPS.
3. The page's JavaScript calls `/v1` as any client would. The browser attaches the access
   cookie, and the API accepts it where it would accept a bearer token.
4. When the access token expires (`TQ_ACCESS_TOKEN_MINUTES`), the page asks
   `POST /ui/session/refresh` for a new pair and repeats the request. Opening a page with
   an expired access token does the same on the server. This is where a disabled account,
   a changed password or a sign-out elsewhere ends the session.
5. **Sign out** revokes every refresh token of the account, as `POST /v1/auth/logout` does,
   and clears the cookies. Access tokens already issued stay valid until they expire.

Accepting a cookie makes cross-site request forgery possible in principle, so:

- The cookies are `SameSite=Lax`: a browser does not send them with another site's form
  post or `fetch`.
- A request authenticated by the cookie that is not a `GET` must also carry the header
  `X-TQ-UI`. The UI sends it. Another origin cannot add a header without a CORS preflight,
  and the preflight is refused because the header is not in the allowed list.
- A request with an `Authorization` header is not affected. The bearer token is used and
  the cookie is ignored.

The pages are sent with a Content-Security-Policy that allows scripts and styles from the
same origin only, and none inline, and forbids framing. Task names, payloads and log lines
are put on the page as text, never as HTML.

If the server only verifies tokens from [your own identity provider](auth.md#using-your-own-identity-provider)
and cannot issue them, nobody can sign in to the UI. The login page says so.

Serve it over HTTPS anywhere but your own machine. Over plain HTTP the cookies are not
marked `Secure` and the password crosses the network in the clear, as with the API.

## How it is built

| | |
|---|---|
| [api/ui.py](../src/pgtaskqueue/api/ui.py) | The routes: pages, login, logout, refresh. |
| [api/templates/](../src/pgtaskqueue/api/templates) | Jinja2 templates. Each page is a shell that names one custom element. |
| [api/static/core.js](../src/pgtaskqueue/api/static/core.js) | The API client, DOM helpers, formatting, polling. |
| [api/static/](../src/pgtaskqueue/api/static) | One file per page, each defining a custom element (`<tq-dashboard>`, `<tq-task-list>`, `<tq-task-detail>`, `<tq-stats>`), plus `<tq-enqueue>` and the chart `<tq-timeline>`. |
| [api/assets.py](../src/pgtaskqueue/api/assets.py) | The version in the static URLs, and the command that bumps it. |

Plain JavaScript modules and one stylesheet, served as they are. There is no build step, no
framework and no dependency fetched from another host. The only package added is Jinja2, in
the `api` extra.

The pages contain no data. Everything shown is fetched from `/v1`, so the UI is one more
client of the API, and the service layer stays the only place a permission is decided.
The chart is drawn from `GET /v1/stats/timeline`, which was added for it and is documented
in [api.md](api.md#queues-workers-and-statistics).

### Static file versions

Pages load their files with the version in the query string, for example
`/ui/static/tasks.js?v=1791957650036`, and the response tells the browser to keep them for a
year without asking again. The templates add the `?v=`. Imports between modules carry it
too, written into the files:

```js
import { api, h } from "./core.js?v=1791957650036";
```

There is no import map to add it instead: the Content-Security-Policy refuses inline
scripts, and an import map is one.

The version is in
[api/static_version.json](../src/pgtaskqueue/api/static_version.json), with a digest of the
files it was set for. After changing anything in `api/static/`, bump it and commit the
result with the change:

```bash
uv run python -m pgtaskqueue.api.assets bump    # prints: static version <old> -> <new>
uv run python -m pgtaskqueue.api.assets check   # exits 1 if the files changed since
```

`bump` sets the version to the current time in milliseconds, so it only grows, and rewrites
the `?v=` of every relative import in `api/static/*.js`. Do not edit those by hand. Every
module must import another by the same URL: the browser keeps one module per URL, so
`core.js?v=1` and `core.js?v=2` would be two copies of it. `test_static_version_matches_the_files`
fails if the files changed without a bump, or if an import carries another version.

A request with another version, or with none, is answered with the current file and
`Cache-Control: no-cache`. During a rolling deploy a page served by an older API can ask a
newer one for its files, and gets an answer.

While you edit files in `api/static/` with the source mounted into the container, the
browser keeps the files of the version it has. Reload with the cache bypassed
(Cmd+Shift+R or Ctrl+Shift+R), or bump.

Behind a proxy that serves the application under a path prefix, start uvicorn with that
`--root-path`; links, scripts and cookie paths follow it. This has not been tested behind a
real proxy.

## Limits

- **It polls.** Pages ask again every few seconds. There are no server-sent events and no
  websockets.
- **The task list refreshes up to 200 rows.** Load more than that and it stops refreshing
  itself until the page is reloaded.
- **Statistics read the tasks table.** A window of 7 days over a table with many millions
  of finished tasks will be slow; [analytics.md](analytics.md) has queries to schedule
  instead.
- **Chart buckets end at the moment of the request**, not on round times, so a bar's
  contents shift slightly between refreshes.
- **No schedules or task editing** in the UI.
- **No rate limit on `POST /ui/login`**, like the token endpoint. Limit both at your proxy.
- **Tested in Chrome only**, in light and dark, at desktop and phone widths. It uses
  nothing newer than `<dialog>`, custom elements and `:has()`.
