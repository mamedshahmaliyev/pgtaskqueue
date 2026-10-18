// <tq-task-detail task-id="…">: one task, kept current, with its log followed live.

import { ACTIVE, actions, api, attempt, badge, can, clock, copyable, h, poll, redraw, root, seconds, when } from "./core.js?v=1791960953255";

const LOG_PAGE = 500;

const json = (value) => h("pre", { class: "json" }, JSON.stringify(value, null, 2));
const isEmpty = (value) => value === null || value === undefined || (typeof value === "object" && !Object.keys(value).length);

class TaskDetail extends HTMLElement {
  lastLog = 0;

  connectedCallback() {
    this.id_ = this.getAttribute("task-id");
    this.head = h("div");
    this.facts = h("div");
    this.data = h("div", { class: "columns" });
    this.log = h("div", { class: "log", tabIndex: 0, role: "log", "aria-label": "Task log" });
    this.logNote = h("p", { class: "empty" }, "No log lines yet.");
    this.append(
      h("p", null, h("a", { href: `${root}/ui/tasks` }, "← Tasks")),
      this.head,
      h("section", { class: "card" }, this.facts),
      this.data,
      h("section", { class: "card" }, h("h2", null, "Log"), this.logNote, this.log),
    );
    this.stop = poll(() => this.load(), 2);
  }

  disconnectedCallback() {
    this.stop?.();
  }

  async load() {
    let task;
    try {
      task = await api(`/tasks/${this.id_}`);
    } catch (error) {
      if (error.status !== 404) throw error;
      // Not there, or not this account's to see: the API gives the same answer for both.
      this.stop();
      this.replaceChildren(h("p", null, h("a", { href: `${root}/ui/tasks` }, "← Tasks")), h("p", { class: "card empty" }, "No such task."));
      return;
    }
    this.task = task;
    redraw(this.head, [task.status, task.name], () => this.drawHead(task));
    redraw(this.facts, task, () => this.drawFacts(task));
    redraw(this.data, [task.payload, task.metadata, task.progress, task.output, task.error], () => this.drawData(task));
    await this.follow();
  }

  /** Append the log lines written since the last look. */
  async follow() {
    for (;;) {
      const page = await api(`/tasks/${this.id_}/logs`, { query: { after_id: this.lastLog, limit: LOG_PAGE } });
      if (!page.items.length) return;
      // Stay at the bottom only if the reader is there; do not pull them away from a line.
      const atBottom = this.log.scrollHeight - this.log.scrollTop - this.log.clientHeight < 40;
      this.logNote.hidden = true;
      this.log.append(
        ...page.items.map((line) =>
          h(
            "div",
            { class: `line ${line.level}` },
            h("time", { title: clock(line.created_at) }, new Date(line.created_at).toLocaleTimeString()),
            h("span", { class: "level" }, line.level),
            h("span", { class: "muted" }, `#${line.attempt}${line.step ? ` ${line.step}` : ""}`),
            h("span", { class: "message" }, line.message, line.data && h("span", { class: "muted" }, ` ${JSON.stringify(line.data)}`)),
          ),
        ),
      );
      this.lastLog = page.last_id;
      if (atBottom) this.log.scrollTop = this.log.scrollHeight;
      if (page.items.length < LOG_PAGE) return;
    }
  }

  drawHead(task) {
    const fromStart = h("input", { type: "checkbox" });
    const run = (key, ...args) => async () => {
      if (key === "stop" && task.status === "running" && !confirm(`Kill ${task.name}?\n\nIt is running and will be interrupted. Work done since the last completed step is lost.`)) return;
      await attempt(() => actions[key].run(task, ...args.map((arg) => arg())), key === "stop" ? "Stopped." : "Queued to run again.");
      await this.load();
    };
    return h(
      "div",
      { class: "heading" },
      h("h1", null, task.name, " ", badge(task.status)),
      can("tasks:manage") &&
        h(
          "div",
          { class: "buttons" },
          actions.stop.applies(task) && h("button", { class: "danger", onclick: run("stop") }, actions.stop.label(task)),
          actions.restart.applies(task) && [
            h("label", { class: "inline", title: "Forget completed steps, so the task starts from the beginning." }, fromStart, "from the first step"),
            h("button", { class: "primary", onclick: run("restart", () => fromStart.checked) }, "Restart"),
          ],
        ),
    );
  }

  drawFacts(task) {
    const running = task.status === "running" && task.started_at;
    const facts = [
      ["Id", copyable(task.id)],
      ["Queue", h("a", { href: `${root}/ui/tasks?queue=${encodeURIComponent(task.queue)}` }, task.queue)],
      ["Priority", task.priority],
      ["Attempt", `${task.attempt} of ${task.max_attempts}`],
      ["Step", task.step],
      ["Created", when(task.created_at)],
      ["Created by", task.created_by],
      [ACTIVE.includes(task.status) && task.status !== "running" ? "Runs" : "Was due", when(task.run_after)],
      ["Held", task.status === "scheduled" && task.capped && "until its account has fewer tasks pending or running than its limit"],
      ["Started", task.started_at && when(task.started_at)],
      ["Last heartbeat", running && when(task.heartbeat_at)],
      ["Finished", task.completed_at && when(task.completed_at)],
      ["Run time, all attempts", task.spent_seconds ? seconds(task.spent_seconds) : null],
      ["Timeout", task.timeout_seconds && seconds(task.timeout_seconds)],
      ["Worker", task.worker_id && h("code", null, task.worker_id)],
      ["Dedupe key", task.dedupe_key],
      ["Callback", task.callback?.url],
    ];
    return h(
      "dl",
      { class: "facts" },
      facts.filter(([, value]) => value !== null && value !== undefined && value !== false && value !== "").map(([label, value]) => h("div", null, h("dt", null, label), h("dd", null, value))),
    );
  }

  drawData(task) {
    const block = (title, value, extra) => !isEmpty(value) && h("section", { class: "card" }, h("h2", null, title), extra, json(value));
    const error = task.error;
    return [
      error &&
        h(
          "section",
          { class: "card wide" },
          h("h2", null, "Error"),
          h("p", { class: "error" }, h("strong", null, error.type ?? "Error"), error.message ? `: ${error.message}` : ""),
          error.traceback && h("pre", { class: "json" }, error.traceback),
        ),
      block("Payload", task.payload),
      block("Output", task.output),
      block("Progress", task.progress),
      block("Metadata", task.metadata),
    ];
  }
}

customElements.define("tq-task-detail", TaskDetail);
