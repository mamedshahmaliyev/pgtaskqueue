// <tq-stats>: how a chosen stretch of time went, per queue and task name.

import "./chart.js?v=1791960953255";
import { api, can, count, h, me, poll, redraw, root, seconds, tile } from "./core.js?v=1791960953255";

// Minutes, a label, and how many bars to cut the window into.
const WINDOWS = [
  [15, "15 minutes", 30],
  [60, "1 hour", 30],
  [360, "6 hours", 36],
  [1440, "24 hours", 48],
  [10080, "7 days", 56],
];

const failureRate = (item) => {
  const settled = item.window.succeeded + item.window.failed;
  return settled ? item.window.failed / settled : null;
};

const COLUMNS = [
  { key: "name", title: "Task", value: (i) => `${i.queue}/${i.name}`, text: true },
  { key: "pending", title: "Pending", value: (i) => i.pending },
  { key: "running", title: "Running", value: (i) => i.running },
  { key: "scheduled", title: "Scheduled", value: (i) => i.scheduled },
  { key: "succeeded", title: "Succeeded", value: (i) => i.window.succeeded },
  { key: "failed", title: "Failed", value: (i) => i.window.failed },
  { key: "canceled", title: "Canceled", value: (i) => i.window.canceled },
  { key: "rate", title: "Failure rate", value: failureRate, show: (v) => (v === null ? "–" : `${(100 * v).toFixed(v && v < 0.1 ? 1 : 0)}%`) },
  { key: "p50", title: "p50", value: (i) => i.window.p50_seconds ?? null, show: seconds },
  { key: "p95", title: "p95", value: (i) => i.window.p95_seconds ?? null, show: seconds },
  { key: "wait", title: "Avg wait", value: (i) => i.window.avg_wait_seconds ?? null, show: seconds },
  { key: "attempts", title: "Avg attempts", value: (i) => i.window.avg_attempts ?? null, show: (v) => (v === null ? "–" : v.toFixed(2)) },
  { key: "oldest", title: "Oldest pending", value: (i) => i.oldest_pending_seconds, show: seconds },
];

class Stats extends HTMLElement {
  sort = { key: "name", down: false };

  connectedCallback() {
    if (!can("stats:read")) {
      this.append(h("h1", null, "Statistics"), h("p", { class: "card muted" }, `The ${me.role} role does not include queue statistics.`));
      return;
    }
    const chosen = Number(new URLSearchParams(location.search).get("window")) || 60;
    this.window = h(
      "select",
      { "aria-label": "Window", onchange: () => this.pick() },
      WINDOWS.map(([minutes, label]) => h("option", { value: minutes, selected: minutes === chosen }, `Last ${label}`)),
    );
    this.totals = h("section", { class: "tiles" });
    this.timeline = h("tq-timeline");
    this.tasks = h("div");
    this.append(
      h("div", { class: "heading" }, h("h1", null, "Statistics"), this.window),
      this.totals,
      h("section", { class: "card" }, h("h2", null, "Finished tasks"), this.timeline),
      h(
        "section",
        { class: "card" },
        h("h2", null, "By queue and task"),
        h("p", { class: "muted" }, "Counts of pending, running and scheduled are as of now. The rest covers the window. Durations are of tasks that succeeded."),
        this.tasks,
      ),
    );
    this.stop = poll(() => this.load(), 10);
  }

  disconnectedCallback() {
    this.stop?.();
  }

  pick() {
    history.replaceState(null, "", `?window=${this.window.value}`);
    return this.load();
  }

  async load() {
    const minutes = Number(this.window.value);
    const buckets = WINDOWS.find(([m]) => m === minutes)?.[2] ?? 30;
    const [stats, timeline] = await Promise.all([
      api("/stats", { query: { window_minutes: minutes } }),
      api("/stats/timeline", { query: { window_minutes: minutes, buckets } }),
    ]);
    this.stats = stats;
    this.timeline.data = timeline;
    redraw(this.totals, stats.totals, (totals) => this.drawTotals(totals, stats.tasks));
    redraw(this.tasks, [stats.tasks, this.sort], () => this.drawTasks());
  }

  drawTotals(totals, tasks) {
    const settled = totals.succeeded + totals.failed;
    const slowest = Math.max(...tasks.map((item) => item.window.p95_seconds ?? 0), 0);
    return [
      tile("Succeeded", count(totals.succeeded)),
      tile("Failed", count(totals.failed), settled ? `${((100 * totals.failed) / settled).toFixed(1)}% of succeeded and failed` : null, totals.failed ? "bad" : ""),
      tile("Canceled", count(totals.canceled)),
      tile("Throughput", `${totals.throughput_per_minute}/min`, "finished, averaged over the window"),
      tile("Slowest p95", slowest ? seconds(slowest) : "–", "among task names"),
      tile("Waiting now", count(totals.pending), totals.oldest_pending_seconds !== null ? `oldest ${seconds(totals.oldest_pending_seconds)}` : null),
    ];
  }

  drawTasks() {
    const items = this.stats.tasks;
    if (!items.length) return h("p", { class: "empty" }, "No tasks waiting, running or finished in this window.");
    const column = COLUMNS.find((c) => c.key === this.sort.key);
    const sorted = [...items].sort((a, b) => {
      const [x, y] = [column.value(a), column.value(b)];
      // Rows with no value go last whichever way the column is sorted.
      if (x === null || y === null) return (x === null) - (y === null);
      const order = column.text ? x.localeCompare(y) : x - y;
      return this.sort.down ? -order : order;
    });
    const header = (c) =>
      h(
        "th",
        { class: c.text ? "" : "num", "aria-sort": this.sort.key === c.key ? (this.sort.down ? "descending" : "ascending") : null },
        h(
          "button",
          {
            class: "sort",
            onclick: () => {
              this.sort = { key: c.key, down: this.sort.key === c.key ? !this.sort.down : !c.text };
              redraw(this.tasks, [this.stats.tasks, this.sort], () => this.drawTasks());
            },
          },
          c.title,
          this.sort.key === c.key && h("span", { "aria-hidden": "true" }, this.sort.down ? " ↓" : " ↑"),
        ),
      );
    const cell = (c, item) => {
      const value = c.value(item);
      if (c.key === "name") {
        const query = new URLSearchParams({ queue: item.queue, name: item.name });
        return h("td", null, h("span", { class: "muted" }, `${item.queue}/`), h("a", { href: `${root}/ui/tasks?${query}` }, item.name));
      }
      return h("td", { class: "num" }, c.show ? c.show(value) : count(value));
    };
    return h(
      "div",
      { class: "scroll" },
      h("table", null, h("thead", null, h("tr", null, COLUMNS.map(header))), h("tbody", null, sorted.map((item) => h("tr", null, COLUMNS.map((c) => cell(c, item)))))),
    );
  }
}

customElements.define("tq-stats", Stats);
