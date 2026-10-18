// <tq-users>: accounts, their roles, ACLs and limits. For an account with `users:manage`.
//
// What may be changed is the server's decision: the listing says which accounts this one
// may manage, and /roles which roles it may give. The page only leaves out the buttons that
// would be refused.

import { api, attempt, count, h, me, poll, redraw, toast, when } from "./core.js?v=1791960953255";

const LIMITS = [
  { key: "max_waiting", label: "Max waiting", hint: "Tasks scheduled or pending at once. More is refused." },
  { key: "max_running", label: "Max running", hint: "Tasks pending or running at once. The rest wait, held." },
  { key: "enqueue_per_minute", label: "Per minute", hint: "Tasks created in any 60 seconds. More is refused." },
  { key: "min_priority", label: "Priority floor", hint: "The most urgent priority it may ask for. Lower runs first." },
];

const csv = (text) =>
  text
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

// A limit as the table shows it: its own value, or the default it falls back to.
function limitText(value, fallback) {
  if (value === null || value === undefined) return fallback ? `${count(fallback)} (default)` : "none (default)";
  return value === 0 ? "none" : count(value);
}

class Users extends HTMLElement {
  users = [];
  roles = [];
  defaults = null;

  connectedCallback() {
    this.body = h("div");
    this.note = h("p", { class: "error", role: "alert", hidden: true });
    this.editor = new AccountDialog(this);
    this.password = new PasswordDialog();
    this.append(
      h(
        "div",
        { class: "heading" },
        h("h1", null, "Accounts"),
        h("button", { class: "primary", onclick: () => this.editor.open(null) }, "New account"),
      ),
      h(
        "p",
        { class: "muted" },
        "A role says what an account may do; its ACL, to which queues and task names. Limits apply to the tasks it enqueues. An empty limit takes the server's default; 0 is no limit.",
      ),
      this.note,
      h("section", { class: "card" }, this.body),
      this.editor.dialog,
      this.password.dialog,
    );
    this.stop = poll(() => this.load(), 15);
  }

  disconnectedCallback() {
    this.stop?.();
  }

  async load() {
    try {
      const [users, roles, mine] = await Promise.all([api("/users"), api("/roles"), api("/auth/limits")]);
      this.users = users;
      this.roles = roles;
      this.defaults = mine.defaults;
      this.note.hidden = true;
      redraw(this.body, users, () => this.draw());
    } catch (error) {
      this.note.textContent = error.status === 403 ? "This account may not manage accounts." : error.message;
      this.note.hidden = false;
    }
  }

  draw() {
    if (!this.users.length) return h("p", { class: "empty" }, "No accounts.");
    const acl = (patterns) => (patterns.includes("*") ? h("span", { class: "muted" }, "any") : patterns.join(", "));
    const row = (user) =>
      h(
        "tr",
        { class: user.is_active ? "" : "inactive" },
        h(
          "td",
          null,
          h("strong", null, user.username),
          user.username === me.subject && h("span", { class: "muted" }, " (you)"),
          h("div", { class: "muted" }, user.kind === "service" ? "service" : "person", !user.is_active && " · disabled"),
        ),
        h("td", null, user.role),
        h("td", null, h("div", null, "queues: ", acl(user.queues)), h("div", null, "tasks: ", acl(user.task_names))),
        LIMITS.map(({ key }) => h("td", { class: "num" }, limitText(user[key], this.defaults?.[key]))),
        h("td", null, when(user.last_login_at)),
        h(
          "td",
          null,
          user.manageable
            ? h(
                "div",
                { class: "row-actions" },
                h("button", { class: "small", onclick: () => this.editor.open(user) }, "Edit"),
                h("button", { class: "small", onclick: () => this.password.open(user) }, user.kind === "service" ? "Secret" : "Password"),
                user.username !== me.subject && h("button", { class: "small danger", onclick: () => this.remove(user) }, "Delete"),
              )
            : h("span", { class: "muted", title: "Its role or ACL reaches beyond this account's." }, "beyond your role"),
        ),
      );
    return h(
      "div",
      { class: "scroll" },
      h(
        "table",
        null,
        h(
          "thead",
          null,
          h(
            "tr",
            null,
            ["Account", "Role", "ACL"].map((title) => h("th", null, title)),
            LIMITS.map(({ label, hint }) => h("th", { class: "num", title: hint }, label)),
            h("th", null, "Last sign-in"),
            h("th", null, ""),
          ),
        ),
        h("tbody", null, this.users.map(row)),
      ),
    );
  }

  async remove(user) {
    if (!confirm(`Delete ${user.username}? Its tasks stay; it can no longer sign in or get a token.`)) return;
    const done = await attempt(() => api(`/users/${encodeURIComponent(user.username)}`, { method: "DELETE" }), `Deleted ${user.username}.`);
    if (done !== undefined) await this.reload();
  }

  async reload() {
    delete this.body.dataset.shown;
    await this.load();
  }
}

/** Create an account, or change one: role, ACL, limits, enabled. */
class AccountDialog {
  constructor(page) {
    this.page = page;
    const field = (label, control, hint) => h("label", null, label, control, hint && h("small", { class: "muted" }, hint));
    this.username = h("input", { required: true, autocomplete: "off", pattern: "[A-Za-z0-9][A-Za-z0-9._@\\-]{0,62}" });
    this.kind = h("select", null, h("option", { value: "user" }, "Person (password)"), h("option", { value: "service" }, "Service (client secret)"));
    this.secret = h("input", { type: "password", minLength: 8, autocomplete: "new-password" });
    this.role = h("select");
    this.queues = h("input", { placeholder: "*" });
    this.names = h("input", { placeholder: "*" });
    this.active = h("input", { type: "checkbox" });
    this.limits = Object.fromEntries(LIMITS.map(({ key }) => [key, h("input", { type: "number", min: 0, max: key === "min_priority" ? 32767 : undefined, step: 1 })]));
    this.usage = h("p", { class: "muted" });
    this.error = h("p", { class: "error", role: "alert", hidden: true });
    this.title = h("h2");
    this.submit = h("button", { type: "submit", class: "primary" });
    this.newOnly = h(
      "div",
      { class: "grid" },
      field("Username", this.username),
      field("Kind", this.kind),
      field("Password or client secret", this.secret, "At least 8 characters."),
    );
    this.activeRow = h("label", { class: "inline" }, this.active, "Enabled. Disabling also ends its sessions.");

    this.dialog = h(
      "dialog",
      null,
      h(
        "form",
        { onsubmit: (event) => this.send(event) },
        this.title,
        this.error,
        this.newOnly,
        h(
          "div",
          { class: "grid" },
          field("Role", this.role),
          field("Queues", this.queues, "Comma-separated globs. * is every queue."),
          field("Task names", this.names, "Comma-separated globs. * is every name."),
        ),
        h("h3", null, "Limits"),
        h(
          "div",
          { class: "grid" },
          LIMITS.map(({ key, label, hint }) => field(label, this.limits[key], hint)),
        ),
        this.usage,
        this.activeRow,
        h("div", { class: "buttons" }, h("button", { type: "button", onclick: () => this.dialog.close() }, "Close"), this.submit),
      ),
    );
  }

  open(user) {
    this.user = user;
    this.error.hidden = true;
    this.title.textContent = user ? `Account ${user.username}` : "New account";
    this.submit.textContent = user ? "Save" : "Create";
    this.newOnly.hidden = Boolean(user);
    for (const input of [this.username, this.secret]) input.required = !user;
    this.username.value = "";
    this.secret.value = "";
    this.kind.value = "user";

    const current = user?.role ?? "submitter";
    this.role.replaceChildren(
      ...this.page.roles
        .filter((role) => role.assignable || role.name === current)
        .map((role) => h("option", { value: role.name, disabled: !role.assignable, title: role.permissions.join(", ") }, role.name)),
    );
    this.role.value = current;
    this.queues.value = (user?.queues ?? ["*"]).join(", ");
    this.names.value = (user?.task_names ?? ["*"]).join(", ");
    const own = user?.username === me.subject;
    // The server refuses an account changing its own role, ACL or (unless admin) limits;
    // say so here rather than after a round trip.
    for (const control of [this.role, this.queues, this.names, ...Object.values(this.limits)]) {
      control.disabled = own && me.role !== "admin";
    }
    this.role.disabled = own;
    for (const { key } of LIMITS) {
      const input = this.limits[key];
      input.value = user?.[key] ?? "";
      const fallback = this.page.defaults?.[key];
      input.placeholder = `default: ${fallback ? fallback : "none"}`;
    }
    this.activeRow.hidden = !user || own;
    this.active.checked = user?.is_active ?? true;
    this.usage.textContent = "";
    this.dialog.showModal();
    if (user) this.showUsage(user);
  }

  async showUsage(user) {
    try {
      const { usage } = await api(`/users/${encodeURIComponent(user.username)}/limits`);
      this.usage.textContent = `Now: ${count(usage.waiting)} waiting${usage.held ? ` (${count(usage.held)} held)` : ""}, ${count(usage.running)} running, ${count(usage.last_minute)} enqueued in the last minute.`;
    } catch {
      // The form works without it.
    }
  }

  body() {
    const body = { role: this.role.value, queues: csv(this.queues.value), task_names: csv(this.names.value) };
    if (!body.queues.length || !body.task_names.length) throw new Error("An ACL needs at least one pattern; use * for everything.");
    for (const { key } of LIMITS) {
      const text = this.limits[key].value.trim();
      // Empty sends null: back to the default.
      body[key] = text === "" ? null : Number(text);
    }
    if (this.user) {
      if (!this.activeRow.hidden) body.is_active = this.active.checked;
      // Only what changed: an account may not send its own role, ACL or limits at all.
      for (const key of Object.keys(body)) {
        const before = this.user[key];
        if (JSON.stringify(before ?? null) === JSON.stringify(body[key])) delete body[key];
      }
      return body;
    }
    return { ...body, username: this.username.value.trim(), kind: this.kind.value, password: this.secret.value };
  }

  async send(event) {
    event.preventDefault();
    this.error.hidden = true;
    this.submit.disabled = true;
    try {
      const body = this.body();
      const user = this.user
        ? await api(`/users/${encodeURIComponent(this.user.username)}`, { method: "PATCH", body })
        : await api("/users", { method: "POST", body });
      toast(this.user ? `Saved ${user.username}.` : `Created ${user.username}.`);
      this.dialog.close();
      await this.page.reload();
    } catch (error) {
      this.error.textContent = error.message;
      this.error.hidden = false;
    } finally {
      this.submit.disabled = false;
    }
  }
}

/** Set an account's password, or a service's client secret. Ends its sessions. */
class PasswordDialog {
  constructor() {
    this.secret = h("input", { type: "password", required: true, minLength: 8, autocomplete: "new-password" });
    this.error = h("p", { class: "error", role: "alert", hidden: true });
    this.title = h("h2");
    this.dialog = h(
      "dialog",
      null,
      h(
        "form",
        { onsubmit: (event) => this.send(event) },
        this.title,
        h("p", { class: "muted" }, "Its sessions and refresh tokens end. Tokens already issued last until they expire."),
        this.error,
        h("label", null, "New value", this.secret, h("small", { class: "muted" }, "At least 8 characters.")),
        h("div", { class: "buttons" }, h("button", { type: "button", onclick: () => this.dialog.close() }, "Close"), h("button", { type: "submit", class: "primary" }, "Set")),
      ),
    );
  }

  open(user) {
    this.user = user;
    this.title.textContent = `${user.kind === "service" ? "Client secret" : "Password"} for ${user.username}`;
    this.secret.value = "";
    this.error.hidden = true;
    this.dialog.showModal();
  }

  async send(event) {
    event.preventDefault();
    try {
      await api(`/users/${encodeURIComponent(this.user.username)}/password`, { method: "POST", body: { password: this.secret.value } });
      toast(`Changed for ${this.user.username}.`);
      this.dialog.close();
    } catch (error) {
      this.error.textContent = error.message;
      this.error.hidden = false;
    }
  }
}

customElements.define("tq-users", Users);
