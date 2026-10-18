// What every page shares: the API client, DOM helpers, formatting, polling.
//
// Nothing here builds HTML from strings. Task names, payloads and log lines are written by
// whoever enqueues tasks, so they only ever reach the page as text nodes.

export const root = document.body.dataset.root ?? "";
export const me = JSON.parse(document.getElementById("tq-me")?.textContent ?? "null");

export const can = (permission) => Boolean(me?.permissions.includes(permission));

export const ACTIVE = ["scheduled", "pending", "running"];
export const STATUSES = [...ACTIVE, "succeeded", "failed", "canceled"];

// ---------------------------------------------------------------------------- API

export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function describe(detail, fallback) {
  if (typeof detail === "string") return detail;
  // FastAPI's validation errors: a list of {loc, msg}.
  if (Array.isArray(detail)) {
    return detail.map((e) => `${(e.loc ?? []).slice(1).join(".")}: ${e.msg}`).join("; ");
  }
  return fallback;
}

let renewing = null;

// One refresh at a time, however many requests found their token expired together.
function renew() {
  renewing ??= fetch(`${root}/ui/session/refresh`, { method: "POST", headers: { "X-TQ-UI": "1" } })
    .then((response) => response.ok)
    .catch(() => false)
    .finally(() => setTimeout(() => (renewing = null), 0));
  return renewing;
}

export async function api(path, { method = "GET", body, query } = {}) {
  const url = new URL(`${root}/v1${path}`, location.origin);
  for (const [key, value] of Object.entries(query ?? {})) {
    for (const item of [value].flat()) {
      if (item !== undefined && item !== null && item !== "") url.searchParams.append(key, item);
    }
  }
  const options = { method, headers: { "X-TQ-UI": "1" } };
  if (body !== undefined) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body);
  }
  let response = await fetch(url, options);
  if (response.status === 401) {
    if (!(await renew())) {
      const here = location.pathname + location.search;
      location.assign(`${root}/ui/login?next=${encodeURIComponent(here)}`);
      throw new ApiError(401, "Your session has ended.");
    }
    response = await fetch(url, options);
  }
  if (response.status === 204) return null;
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    throw new ApiError(response.status, describe(data?.detail, `The server answered ${response.status}.`));
  }
  return data;
}

// ---------------------------------------------------------------------------- DOM

const SVG_NS = "http://www.w3.org/2000/svg";

// Names whose DOM property is read-only or means something else than the attribute.
const ATTRIBUTE_ONLY = new Set(["list", "form", "for", "role"]);

function build(node, attrs, children) {
  for (const [key, value] of Object.entries(attrs ?? {})) {
    if (value === false || value === null || value === undefined) continue;
    if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else if (key === "class") node.setAttribute("class", value);
    else if (key in node && !ATTRIBUTE_ONLY.has(key) && !(node instanceof SVGElement)) node[key] = value;
    else node.setAttribute(key, value === true ? "" : value);
  }
  for (const child of children.flat(Infinity)) {
    if (child === false || child === null || child === undefined) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

export const h = (tag, attrs, ...children) => build(document.createElement(tag), attrs, children);
export const svg = (tag, attrs, ...children) =>
  build(document.createElementNS(SVG_NS, tag), attrs, children);

export function toast(message, kind = "info") {
  const note = h("div", { class: `toast ${kind}` }, message);
  document.getElementById("toasts").append(note);
  setTimeout(() => note.remove(), kind === "error" ? 8000 : 4000);
}

/** Run an action a button started; say what went wrong if it fails. */
export async function attempt(action, done) {
  try {
    const result = await action();
    if (done) toast(done);
    return result;
  } catch (error) {
    toast(error.message, "error");
    return undefined;
  }
}

/**
 * Call `load` now and every `seconds`, but not while the tab is hidden, and never twice at
 * once. Returns a function that stops it.
 */
export function poll(load, seconds) {
  let timer = null;
  let stopped = false;
  let busy = false;
  const tick = async () => {
    if (stopped || busy || document.hidden) return;
    busy = true;
    try {
      await load();
    } catch (error) {
      console.warn(error);
    } finally {
      busy = false;
    }
  };
  const onVisible = () => !document.hidden && tick();
  document.addEventListener("visibilitychange", onVisible);
  timer = setInterval(tick, seconds * 1000);
  tick();
  return () => {
    stopped = true;
    clearInterval(timer);
    document.removeEventListener("visibilitychange", onVisible);
  };
}

/**
 * Redraw a region only when what it shows has changed, so a refresh every few seconds does
 * not take the focus, the text selection or a hovered button away from the reader.
 */
export function redraw(host, data, render) {
  const key = JSON.stringify(data);
  if (host.dataset.shown === key) return;
  host.dataset.shown = key;
  // Through h(), which drops the `false` a `condition && node` leaves behind.
  host.replaceChildren(...h("div", null, render(data)).childNodes);
}

// ---------------------------------------------------------------------------- formatting

export const count = (n) => (n ?? 0).toLocaleString("en-US");

export function seconds(value) {
  if (value === null || value === undefined) return "–";
  if (value < 1) return `${Math.round(value * 1000)} ms`;
  if (value < 60) return `${value.toFixed(value < 10 ? 1 : 0)} s`;
  if (value < 3600) return `${Math.floor(value / 60)}m ${Math.round(value % 60)}s`;
  if (value < 86400) return `${Math.floor(value / 3600)}h ${Math.round((value % 3600) / 60)}m`;
  return `${Math.floor(value / 86400)}d ${Math.round((value % 86400) / 3600)}h`;
}

export function ago(iso) {
  const delta = (Date.now() - new Date(iso).getTime()) / 1000;
  if (Math.abs(delta) < 5) return "just now";
  return delta > 0 ? `${seconds(delta)} ago` : `in ${seconds(-delta)}`;
}

export const clock = (iso) =>
  new Date(iso).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "medium" });

// A status is never shown by colour alone: each has its own mark and its name.
const MARKS = {
  scheduled: "◷",
  pending: "○",
  running: "◐",
  succeeded: "✓",
  failed: "✕",
  canceled: "⊘",
};

export const badge = (status) =>
  h("span", { class: `badge ${status}` }, h("span", { "aria-hidden": "true" }, MARKS[status] ?? "•"), status);

/** A moment, shown as how long ago it was and kept current while it is on the page. */
class Time extends HTMLElement {
  static observedAttributes = ["at"];

  connectedCallback() {
    this.show();
    this.timer = setInterval(() => this.show(), 5000);
  }

  disconnectedCallback() {
    clearInterval(this.timer);
  }

  attributeChangedCallback() {
    this.show();
  }

  show() {
    const at = this.getAttribute("at");
    this.textContent = at ? ago(at) : "–";
    if (at) this.title = clock(at);
  }
}
customElements.define("tq-time", Time);

export const when = (iso) => (iso ? h("tq-time", { at: iso }) : "–");

const copyIcon = () =>
  svg(
    "svg",
    { viewBox: "0 0 16 16", width: 13, height: 13, "aria-hidden": "true" },
    svg("rect", { x: 5.5, y: 5.5, width: 8, height: 8, rx: 1.5 }),
    svg("path", { d: "M10.5 3.5v-1a1 1 0 0 0-1-1h-6a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h1" }),
  );

/** A task id in full, with a button that puts it on the clipboard. */
export const copyable = (id) =>
  h(
    "span",
    { class: "copyable" },
    h("code", null, id),
    h(
      "button",
      {
        type: "button",
        class: "copy",
        title: "Copy id",
        "aria-label": `Copy id ${id}`,
        // The clipboard API exists only in a secure context (https, or localhost).
        onclick: () =>
          navigator.clipboard
            ? attempt(() => navigator.clipboard.writeText(id), "Copied the id.")
            : toast("The browser allows copying only over https.", "error"),
      },
      copyIcon(),
    ),
  );

export const taskLink = (task, text) =>
  h("a", { href: `${root}/ui/tasks/${task.id}`, title: task.id }, text ?? task.name);

const icon = (...paths) =>
  svg(
    "svg",
    { viewBox: "0 0 16 16", width: 14, height: 14, "aria-hidden": "true" },
    ...paths.map((d) => svg("path", { d })),
  );

/** What the buttons on a task do, and when each applies. */
export const actions = {
  // "Kill" is cancelling a task that is running: the worker stops it, thread or coroutine. The same call
  // as cancelling one that has not started; the name tells the reader work will be lost.
  stop: {
    label: (task) => (task.status === "running" ? "Kill" : "Cancel"),
    icon: (task) =>
      task.status === "running"
        ? icon("M4.5 4.5h7v7h-7z")
        : icon("M8 1.75a6.25 6.25 0 1 0 0 12.5a6.25 6.25 0 1 0 0-12.5", "M3.6 3.6l8.8 8.8"),
    applies: (task) => ACTIVE.includes(task.status),
    run: (task) => api(`/tasks/${task.id}/cancel`, { method: "POST" }),
  },
  restart: {
    label: () => "Restart",
    icon: () => icon("M13.25 8a5.25 5.25 0 1 1-1.54-3.71", "M12 1.75v2.75h-2.75"),
    applies: (task) => !ACTIVE.includes(task.status),
    run: (task, resetProgress = false) =>
      api(`/tasks/${task.id}/requeue`, { method: "POST", body: { reset_progress: resetProgress } }),
  },
};

export function table(columns, rows, empty = "Nothing to show.") {
  if (!rows.length) return h("p", { class: "empty" }, empty);
  return h(
    "div",
    { class: "scroll" },
    h(
      "table",
      null,
      h("thead", null, h("tr", null, columns.map((c) => h("th", { class: c.num && "num" }, c.title)))),
      h(
        "tbody",
        null,
        rows.map((row) =>
          h("tr", null, columns.map((c) => h("td", { class: c.num && "num" }, c.cell(row)))),
        ),
      ),
    ),
  );
}

export const tile = (label, value, note, kind) =>
  h(
    "div",
    { class: `tile ${kind ?? ""}` },
    h("div", { class: "label" }, label),
    h("div", { class: "value" }, value),
    note && h("div", { class: "note" }, note),
  );
