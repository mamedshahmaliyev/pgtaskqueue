// <tq-task-list>: every task the account may see, filtered, with the actions on them.
//
// The filters live in the address bar, so a filtered view can be bookmarked and shared,
// and the dashboard can link straight into one.

import "./enqueue.js?v=1791960953255";
import { actions, api, badge, can, copyable, h, poll, redraw, root, seconds, STATUSES, taskLink, toast, when } from "./core.js?v=1791960953255";

const PAGE = 50;
// The most rows one request may ask for. Past this the list no longer refreshes itself.
const REFRESH_LIMIT = 200;
const TEXT_FILTERS = [
  ["name", "Task name", "report.build"],
  ["queue", "Queue", "default"],
  ["created_by", "Created by", "alice"],
  ["worker_id", "Worker", ""],
  ["metadata", "Metadata contains", '{"tenant": "acme"}'],
];

class TaskList extends HTMLElement {
  items = [];
  next = null;
  selected = new Set();

  connectedCallback() {
    const params = new URLSearchParams(location.search);
    this.statusBoxes = STATUSES.map((status) =>
      h("label", { class: "chip" }, h("input", { type: "checkbox", value: status, checked: params.getAll("status").includes(status), onchange: () => this.apply() }), badge(status)),
    );
    this.inputs = Object.fromEntries(
      TEXT_FILTERS.map(([key, , placeholder]) => [key, h("input", { value: params.get(key) ?? "", placeholder, oninput: () => this.applySoon() })]),
    );
    this.enqueue = h("tq-enqueue", { onenqueued: () => this.reload() });
    this.bulk = h("div", { class: "bulk" });
    this.body = h("div");
    this.more = h("div", { class: "more-row" });
    this.note = h("p", { class: "error", role: "alert", hidden: true });

    this.append(
      h(
        "div",
        { class: "heading" },
        h("h1", null, "Tasks"),
        can("tasks:enqueue") && h("button", { class: "primary", onclick: () => this.enqueue.open() }, "Enqueue a task"),
      ),
      h(
        "section",
        { class: "card filters" },
        h("div", { class: "chips", role: "group", "aria-label": "Status" }, this.statusBoxes),
        h(
          "div",
          { class: "grid" },
          TEXT_FILTERS.map(([key, label]) => h("label", null, label, this.inputs[key])),
          h("div", { class: "buttons end" }, h("button", { onclick: () => this.reset() }, "Clear filters")),
        ),
      ),
      this.note,
      h("section", { class: "card" }, this.bulk, this.body, this.more),
      this.enqueue,
    );
    this.stop = poll(() => this.refresh(), 5);
  }

  disconnectedCallback() {
    this.stop?.();
  }

  // ------------------------------------------------------------------ filters

  filters() {
    const query = { status: this.statusBoxes.map((box) => box.control).filter((box) => box.checked).map((box) => box.value) };
    for (const [key] of TEXT_FILTERS) query[key] = this.inputs[key].value.trim();
    return query;
  }

  applySoon() {
    clearTimeout(this.typing);
    this.typing = setTimeout(() => this.apply(), 350);
  }

  apply() {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(this.filters())) {
      for (const item of [value].flat()) if (item) params.append(key, item);
    }
    history.replaceState(null, "", params.size ? `?${params}` : location.pathname);
    this.selected.clear();
    return this.reload();
  }

  reset() {
    for (const box of this.statusBoxes) box.control.checked = false;
    for (const input of Object.values(this.inputs)) input.value = "";
    return this.apply();
  }

  // ------------------------------------------------------------------ loading

  async fetch(query) {
    try {
      const page = await api("/tasks", { query: { ...this.filters(), ...query } });
      this.note.hidden = true;
      return page;
    } catch (error) {
      // A half-typed metadata filter is the usual reason; say so and keep what is shown.
      this.note.textContent = error.message;
      this.note.hidden = false;
      return null;
    }
  }

  async reload() {
    const page = await this.fetch({ limit: PAGE });
    if (page) this.show(page.items, page.next_before);
  }

  /** Read again as many rows as are on the page, so statuses stay current. */
  async refresh() {
    if (this.items.length > REFRESH_LIMIT) return;
    const limit = Math.max(PAGE, this.items.length);
    const page = await this.fetch({ limit });
    if (page) this.show(page.items, page.next_before);
  }

  async loadMore() {
    const page = await this.fetch({ limit: PAGE, before: this.next });
    if (page) this.show([...this.items, ...page.items], page.next_before);
  }

  // ------------------------------------------------------------------ drawing

  show(items, next) {
    this.items = items;
    this.next = next;
    const known = new Set(items.map((task) => task.id));
    for (const id of this.selected) if (!known.has(id)) this.selected.delete(id);
    redraw(this.body, items, () => this.draw());
    this.drawBulk();
    redraw(this.more, [next, items.length], () => [
      h("span", { class: "muted" }, `${items.length} shown`, items.length > REFRESH_LIMIT && ". Too many to refresh automatically; reload the page to start over."),
      next && h("button", { onclick: () => this.loadMore() }, "Load older"),
    ]);
  }

  draw() {
    if (!this.items.length) return h("p", { class: "empty" }, "No tasks match.");
    const manage = can("tasks:manage");
    const all = h("input", {
      type: "checkbox",
      "aria-label": "Select all",
      checked: this.items.every((task) => this.selected.has(task.id)),
      onchange: () => {
        for (const task of this.items) all.checked ? this.selected.add(task.id) : this.selected.delete(task.id);
        for (const box of this.body.querySelectorAll("tbody input")) box.checked = all.checked;
        this.drawBulk();
      },
    });
    const row = (task) =>
      h(
        "tr",
        null,
        manage &&
          h(
            "td",
            null,
            h("input", {
              type: "checkbox",
              "aria-label": `Select ${task.name} ${task.id}`,
              checked: this.selected.has(task.id),
              onchange: (event) => {
                event.target.checked ? this.selected.add(task.id) : this.selected.delete(task.id);
                this.drawBulk();
              },
            }),
          ),
        h("td", null, badge(task.status)),
        h("td", null, taskLink(task), h("div", { class: "muted" }, copyable(task.id))),
        h("td", null, task.queue),
        h("td", { class: "num" }, `${task.attempt} / ${task.max_attempts}`),
        h("td", null, when(task.created_at)),
        h("td", { class: "num" }, task.status === "running" || task.spent_seconds ? seconds(this.spent(task)) : "–"),
        h("td", null, task.created_by ?? "–"),
        h("td", null, h("span", { class: "clip short", title: task.step ?? task.error?.message ?? "" }, task.step ?? task.error?.message ?? "")),
        manage &&
          h(
            "td",
            null,
            h(
              "div",
              { class: "row-actions" },
              Object.entries(actions)
                .filter(([, action]) => action.applies(task))
                .map(([key, action]) =>
                  h(
                    "button",
                    {
                      type: "button",
                      class: `icon ${action.label(task) === "Kill" ? "danger" : ""}`,
                      title: action.label(task),
                      "aria-label": `${action.label(task)} ${task.name} ${task.id}`,
                      onclick: () => this.act(key, [task]),
                    },
                    action.icon(task),
                  ),
                ),
            ),
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
            manage && h("th", null, all),
            ["Status", "Task", "Queue"].map((title) => h("th", null, title)),
            h("th", { class: "num" }, "Attempt"),
            h("th", null, "Created"),
            h("th", { class: "num" }, "Run time"),
            h("th", null, "Created by"),
            h("th", null, "Step or error"),
            manage && h("th", null, ""),
          ),
        ),
        h("tbody", null, this.items.map(row)),
      ),
    );
  }

  // A running task's time so far is what earlier attempts took plus this one.
  spent(task) {
    if (task.status !== "running" || !task.started_at) return task.spent_seconds;
    return task.spent_seconds + (Date.now() - new Date(task.started_at).getTime()) / 1000;
  }

  drawBulk() {
    const chosen = this.items.filter((task) => this.selected.has(task.id));
    const button = (key, label, danger) => {
      const fit = chosen.filter((task) => actions[key].applies(task));
      return h("button", { class: danger ? "danger" : "", disabled: !fit.length, onclick: () => this.act(key, fit) }, `${label} (${fit.length})`);
    };
    this.bulk.hidden = !chosen.length;
    this.bulk.replaceChildren(
      h("span", null, `${chosen.length} selected`),
      button("stop", "Cancel or kill", true),
      button("restart", "Restart"),
    );
  }

  // ------------------------------------------------------------------ actions

  async act(key, tasks) {
    const action = actions[key];
    const running = tasks.filter((task) => task.status === "running").length;
    if (key === "stop" && (running || tasks.length > 1)) {
      const what = tasks.length === 1 ? `Kill ${tasks[0].name}?` : `Cancel ${tasks.length} tasks?`;
      const lost = running ? `\n\n${running === 1 && tasks.length === 1 ? "It is" : `${running} of them are`} running and will be interrupted. Work done since the last completed step is lost.` : "";
      if (!confirm(what + lost)) return;
    }
    if (key === "restart" && tasks.length > 1 && !confirm(`Run ${tasks.length} finished tasks again?`)) return;
    let done = 0;
    const failures = [];
    for (const task of tasks) {
      try {
        await action.run(task);
        done += 1;
      } catch (error) {
        failures.push(`${task.name} ${task.id}: ${error.message}`);
      }
    }
    if (done) toast(`${key === "stop" ? "Stopped" : "Restarted"} ${done} task${done === 1 ? "" : "s"}.`);
    if (failures.length) toast(failures.slice(0, 3).join("\n") + (failures.length > 3 ? `\n…and ${failures.length - 3} more` : ""), "error");
    this.selected.clear();
    await this.refresh();
  }
}

customElements.define("tq-task-list", TaskList);
