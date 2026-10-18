// <tq-dashboard>: where the queue stands right now.

import "./chart.js?v=1791960953255";
import { api, attempt, badge, can, count, h, me, poll, redraw, root, seconds, table, taskLink, tile, when } from "./core.js?v=1791960953255";

const WINDOW_MINUTES = 60;

class Dashboard extends HTMLElement {
  connectedCallback() {
    this.totals = h("section", { class: "tiles" });
    this.timeline = h("tq-timeline");
    this.queues = h("div");
    this.workers = h("div");
    this.running = h("div");
    this.failed = h("div");
    const panel = (title, body, more) =>
      h("section", { class: "card" }, h("h2", null, title, more && h("span", { class: "more" }, more)), body);

    this.append(h("h1", null, "Dashboard"));
    if (can("stats:read")) {
      this.append(
        this.totals,
        panel(`Finished in the last ${WINDOW_MINUTES} minutes`, this.timeline, h("a", { href: `${root}/ui/stats` }, "Statistics")),
        panel("Queues", this.queues),
      );
    } else {
      this.append(
        h("p", { class: "card muted" }, `The ${me.role} role does not include queue statistics. Your own tasks are below.`),
      );
    }
    if (can("workers:read")) this.append(panel("Workers", this.workers));
    if (can("tasks:read")) {
      this.append(
        h(
          "div",
          { class: "columns" },
          panel("Running now", this.running, h("a", { href: `${root}/ui/tasks?status=running` }, "All running")),
          panel("Latest failures", this.failed, h("a", { href: `${root}/ui/tasks?status=failed` }, "All failed")),
        ),
      );
    }
    this.stop = poll(() => this.load(), 5);
  }

  disconnectedCallback() {
    this.stop?.();
  }

  async load() {
    const jobs = [];
    if (can("stats:read")) {
      jobs.push(
        api("/stats", { query: { window_minutes: WINDOW_MINUTES } }).then((stats) => redraw(this.totals, stats.totals, (t) => this.drawTotals(t))),
        api("/stats/timeline", { query: { window_minutes: WINDOW_MINUTES, buckets: 30 } }).then((data) => (this.timeline.data = data)),
        api("/queues").then((queues) => redraw(this.queues, queues, (q) => this.drawQueues(q))),
      );
    }
    if (can("workers:read")) {
      jobs.push(api("/workers").then((workers) => redraw(this.workers, workers, (w) => this.drawWorkers(w))));
    }
    if (can("tasks:read")) {
      jobs.push(
        api("/tasks", { query: { status: "running", limit: 8 } }).then((page) => redraw(this.running, page.items, (items) => this.drawTasks(items, "running"))),
        api("/tasks", { query: { status: "failed", limit: 8 } }).then((page) => redraw(this.failed, page.items, (items) => this.drawTasks(items, "failed"))),
      );
    }
    await Promise.all(jobs);
  }

  drawTotals(totals) {
    const finished = totals.succeeded + totals.failed + totals.canceled;
    const rate = finished ? `${Math.round((100 * totals.failed) / finished)}% of what finished` : null;
    return [
      tile("Pending", count(totals.pending), totals.oldest_pending_seconds !== null && `oldest waiting ${seconds(totals.oldest_pending_seconds)}`),
      tile("Running", count(totals.running)),
      tile("Scheduled", count(totals.scheduled), totals.held ? `${count(totals.held)} held by account limits` : null),
      tile("Succeeded", count(totals.succeeded), "last hour"),
      tile("Failed", count(totals.failed), rate ?? "last hour", totals.failed ? "bad" : ""),
      tile("Throughput", `${totals.throughput_per_minute}/min`, "last hour"),
    ];
  }

  drawQueues(queues) {
    const toggle = (queue) =>
      can("queues:manage") &&
      h(
        "button",
        {
          class: "small",
          onclick: async () => {
            await attempt(
              () => api(`/queues/${encodeURIComponent(queue.name)}/${queue.paused ? "resume" : "pause"}`, { method: "POST" }),
              `Queue ${queue.name} ${queue.paused ? "resumed" : "paused"}.`,
            );
            await this.load();
          },
        },
        queue.paused ? "Resume" : "Pause",
      );
    return table(
      [
        { title: "Queue", cell: (q) => h("a", { href: `${root}/ui/tasks?queue=${encodeURIComponent(q.name)}` }, q.name) },
        { title: "State", cell: (q) => (q.paused ? h("span", { class: "badge canceled", title: q.note ?? "" }, "⏸ paused") : "claiming") },
        { title: "Pending", num: true, cell: (q) => count(q.pending) },
        { title: "Running", num: true, cell: (q) => count(q.running) },
        { title: "Scheduled", num: true, cell: (q) => count(q.scheduled) },
        { title: "Workers", num: true, cell: (q) => count(q.workers) },
        { title: "Slots", num: true, cell: (q) => count(q.slots) },
        { title: "", cell: toggle },
      ],
      queues,
      "No queue has a task waiting, a worker or a pause switch.",
    );
  }

  drawWorkers(workers) {
    return table(
      [
        { title: "Worker", cell: (w) => h("a", { href: `${root}/ui/tasks?worker_id=${encodeURIComponent(w.id)}` }, w.id) },
        { title: "Queue", cell: (w) => w.queue },
        { title: "Host", cell: (w) => `${w.hostname} (${w.pid})` },
        { title: "Busy", num: true, cell: (w) => `${w.in_flight} / ${w.slots}` },
        { title: "Done", num: true, cell: (w) => count(w.counters.succeeded) },
        { title: "Failed", num: true, cell: (w) => count(w.counters.failed) },
        { title: "Started", cell: (w) => when(w.started_at) },
        { title: "Version", cell: (w) => w.version ?? "–" },
      ],
      workers,
      "No worker is alive. Tasks wait until one starts.",
    );
  }

  drawTasks(tasks, kind) {
    return table(
      [
        { title: "Task", cell: (t) => taskLink(t) },
        { title: "Status", cell: (t) => badge(t.status) },
        kind === "running"
          ? { title: "Step", cell: (t) => t.step ?? "–" }
          : { title: "Error", cell: (t) => h("span", { class: "clip" }, t.error?.message ?? t.error?.type ?? "–") },
        kind === "running" ? { title: "Started", cell: (t) => when(t.started_at) } : { title: "Failed", cell: (t) => when(t.completed_at) },
      ],
      tasks,
      kind === "running" ? "Nothing is running." : "No failed tasks.",
    );
  }
}

customElements.define("tq-dashboard", Dashboard);
