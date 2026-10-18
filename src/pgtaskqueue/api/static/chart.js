// <tq-timeline>: finished tasks over time, as stacked bars. Set `.data` to the answer of
// GET /v1/stats/timeline.
//
// The three series are states, so they wear the status colours the rest of the UI uses for
// them, and they are named in a legend, in the tooltip and in the table under the chart:
// the colour is never the only thing that says which is which.

import { clock, count, h, svg } from "./core.js?v=1791960953255";

const SERIES = ["succeeded", "failed", "canceled"];
const HEIGHT = 200;
const MARGIN = { top: 10, right: 8, bottom: 24, left: 40 };
const GAP = 2;

function niceMax(value) {
  if (value <= 4) return 4;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  for (const step of [1, 2, 4, 5, 10]) {
    if (value <= step * magnitude) return step * magnitude;
  }
  return 10 * magnitude;
}

// A bar segment with only its top corners rounded: the data end, not the baseline.
function segment(x, y, width, height, radius) {
  const r = Math.max(0, Math.min(radius, width / 2, height));
  return (
    `M${x},${y + height}V${y + r}a${r},${r} 0 0 1 ${r},${-r}` +
    `h${width - 2 * r}a${r},${r} 0 0 1 ${r},${r}V${y + height}Z`
  );
}

class Timeline extends HTMLElement {
  #data = null;

  connectedCallback() {
    this.classList.add("chart");
    this.observer = new ResizeObserver(() => this.draw());
    this.observer.observe(this);
  }

  disconnectedCallback() {
    this.observer.disconnect();
  }

  set data(value) {
    const key = JSON.stringify(value);
    if (key === this.shown) return;
    this.shown = key;
    this.#data = value;
    this.draw();
  }

  label(bucket, long = false) {
    const start = new Date(bucket.start);
    if (long) return clock(bucket.start);
    return this.#data.window_minutes > 24 * 60
      ? start.toLocaleDateString(undefined, { month: "short", day: "numeric" })
      : start.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  }

  draw() {
    const data = this.#data;
    const width = this.clientWidth;
    if (!data || !width) return;
    const buckets = data.buckets;
    const plotWidth = width - MARGIN.left - MARGIN.right;
    const plotHeight = HEIGHT - MARGIN.top - MARGIN.bottom;
    const totals = buckets.map((b) => SERIES.reduce((sum, key) => sum + b[key], 0));
    const top = niceMax(Math.max(...totals));
    const band = plotWidth / buckets.length;
    const barWidth = Math.max(1, Math.min(band - GAP, 28));
    const y = (value) => MARGIN.top + plotHeight * (1 - value / top);

    const chart = svg("svg", {
      viewBox: `0 0 ${width} ${HEIGHT}`,
      width,
      height: HEIGHT,
      role: "img",
      "aria-label": `Tasks finished in each of the last ${buckets.length} intervals. The table below has the numbers.`,
    });

    for (const tick of [0, top / 4, top / 2, (3 * top) / 4, top]) {
      if (!Number.isInteger(tick)) continue;
      chart.append(
        svg("line", { class: tick ? "grid" : "axis", x1: MARGIN.left, x2: width - MARGIN.right, y1: y(tick), y2: y(tick) }),
        svg("text", { class: "tick", x: MARGIN.left - 6, y: y(tick) + 4, "text-anchor": "end" }, count(tick)),
      );
    }

    const labelEvery = Math.ceil(buckets.length / Math.max(2, Math.floor(plotWidth / 90)));
    const highlight = svg("rect", { class: "hover", y: MARGIN.top, height: plotHeight, width: band, visibility: "hidden" });
    chart.append(highlight);

    buckets.forEach((bucket, index) => {
      const x = MARGIN.left + index * band + (band - barWidth) / 2;
      let stacked = 0;
      const present = SERIES.filter((key) => bucket[key] > 0);
      for (const key of present) {
        const bottom = y(stacked);
        stacked += bucket[key];
        const last = key === present.at(-1);
        // The gap between two segments is taken from the upper one, in surface colour.
        const height = Math.max(1, bottom - y(stacked) - (stacked > bucket[key] ? GAP : 0));
        chart.append(
          svg("path", { class: `seg ${key}`, d: segment(x, bottom - (stacked > bucket[key] ? GAP : 0) - height, barWidth, height, last ? 4 : 0) }),
        );
      }
      if (index % labelEvery === 0) {
        chart.append(
          svg("text", { class: "tick", x: MARGIN.left + index * band + band / 2, y: HEIGHT - 6, "text-anchor": "middle" }, this.label(bucket)),
        );
      }
    });

    // One target per interval, as tall as the plot: easier to hit than a short bar.
    const tip = h("div", { class: "tip", hidden: true });
    buckets.forEach((bucket, index) => {
      const x = MARGIN.left + index * band;
      const target = svg("rect", { class: "target", x, y: MARGIN.top, width: band, height: plotHeight });
      target.addEventListener("pointerenter", () => {
        highlight.setAttribute("x", x);
        highlight.setAttribute("visibility", "visible");
        tip.replaceChildren(
          h("div", { class: "tip-title" }, `${this.label(bucket, true)}, ${Math.round(data.bucket_seconds / 60 * 10) / 10} min`),
          ...SERIES.map((key) =>
            h("div", { class: "tip-row" }, h("span", { class: `swatch ${key}` }), key, h("strong", null, count(bucket[key]))),
          ),
        );
        tip.hidden = false;
        // Beside the interval, not over it: the bars being read stay visible.
        const right = x + band + 8;
        const left = right + tip.offsetWidth <= width ? right : Math.max(x - tip.offsetWidth - 8, 0);
        tip.style.left = `${left}px`;
      });
      target.addEventListener("pointerleave", () => {
        tip.hidden = true;
        highlight.setAttribute("visibility", "hidden");
      });
      chart.append(target);
    });

    const open = this.querySelector("details")?.open ?? false;
    this.replaceChildren(
      h("div", { class: "legend" }, SERIES.map((key) => h("span", null, h("span", { class: `swatch ${key}` }), key))),
      h("div", { class: "plot" }, chart, tip, !Math.max(...totals) && h("p", { class: "empty over" }, "Nothing finished in this window.")),
      h(
        "details",
        { open },
        h("summary", null, "Show as a table"),
        h(
          "div",
          { class: "scroll short" },
          h(
            "table",
            null,
            h("thead", null, h("tr", null, h("th", null, "From"), SERIES.map((key) => h("th", { class: "num" }, key)))),
            h("tbody", null, buckets.map((b) => h("tr", null, h("td", null, this.label(b, true)), SERIES.map((key) => h("td", { class: "num" }, count(b[key])))))),
          ),
        ),
      ),
    );
  }
}

customElements.define("tq-timeline", Timeline);
