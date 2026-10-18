// <tq-enqueue>: a dialog that adds one task. Fires "enqueued" with the task when it has.

import { api, can, h, root, toast } from "./core.js?v=1791960953255";

// A starting payload from a task type's JSON Schema: every property, with its default or
// an empty value of its type, so the form shows what the executor expects.
function skeleton(schema) {
  const blank = { string: "", integer: 0, number: 0, boolean: false, array: [], object: {} };
  const payload = {};
  for (const [key, property] of Object.entries(schema?.properties ?? {})) {
    const type = [property.type].flat()[0];
    payload[key] = "default" in property ? property.default : (blank[type] ?? null);
  }
  return payload;
}

function parseObject(text, what) {
  if (!text.trim()) return {};
  let value;
  try {
    value = JSON.parse(text);
  } catch (error) {
    throw new Error(`${what} is not valid JSON: ${error.message}`);
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${what} must be a JSON object, like {"key": "value"}.`);
  }
  return value;
}

const number = (input) => (input.value.trim() === "" ? undefined : Number(input.value));

class Enqueue extends HTMLElement {
  types = [];

  connectedCallback() {
    const field = (label, control, hint) => h("label", null, label, control, hint && h("small", { class: "muted" }, hint));
    this.name = h("input", { name: "name", required: true, list: "tq-task-names", autocomplete: "off", placeholder: "demo.echo" });
    this.names = h("datalist", { id: "tq-task-names" });
    this.description = h("small", { class: "muted" });
    this.queue = h("input", { name: "queue", value: "default", required: true });
    this.payload = h("textarea", { name: "payload", rows: 7, spellcheck: false, value: "{}" });
    this.metadata = h("textarea", { name: "metadata", rows: 2, spellcheck: false, placeholder: '{"tenant": "acme"}' });
    this.priority = h("input", { type: "number", min: 0, max: 32767, placeholder: "100" });
    this.delay = h("input", { type: "number", min: 0, step: "any", placeholder: "0" });
    this.maxAttempts = h("input", { type: "number", min: 1, max: 100, placeholder: "default" });
    this.timeout = h("input", { type: "number", min: 0, step: "any", placeholder: "none" });
    this.dedupe = h("input", { maxLength: 256 });
    this.error = h("p", { class: "error", role: "alert", hidden: true });
    this.submit = h("button", { type: "submit", class: "primary" }, "Enqueue");

    this.dialog = h(
      "dialog",
      null,
      h(
        "form",
        { onsubmit: (event) => this.send(event) },
        h("h2", null, "Enqueue a task"),
        this.error,
        h("label", null, "Task name", this.name, this.names, this.description),
        field("Payload", this.payload, "A JSON object. Checked against the task's schema when a worker has published one."),
        h(
          "div",
          { class: "grid" },
          field("Queue", this.queue),
          field("Priority", this.priority, "Lower runs first."),
          field("Delay, seconds", this.delay),
          field("Max attempts", this.maxAttempts),
          field("Timeout, seconds", this.timeout),
          field("Dedupe key", this.dedupe),
        ),
        field("Metadata", this.metadata, "Labels to filter by later."),
        h("div", { class: "buttons" }, h("button", { type: "button", onclick: () => this.dialog.close() }, "Close"), this.submit),
      ),
    );
    this.append(this.dialog);
    this.name.addEventListener("change", () => this.pick());
  }

  async open() {
    this.error.hidden = true;
    this.dialog.showModal();
    if (!can("tasks:enqueue") && !can("tasks:read")) return;
    try {
      this.types = await api("/task-types");
      this.names.replaceChildren(...this.types.map((type) => h("option", { value: type.name })));
    } catch {
      // The form works without the list; the name is then typed by hand.
    }
  }

  pick() {
    const type = this.types.find((candidate) => candidate.name === this.name.value);
    this.description.textContent = type
      ? (type.description ?? "A worker has registered this task.")
      : this.name.value && this.types.length
        ? "No worker has registered a task with this name."
        : "";
    if (type?.payload_schema && ["", "{}"].includes(this.payload.value.trim())) {
      this.payload.value = JSON.stringify(skeleton(type.payload_schema), null, 2);
    }
  }

  async send(event) {
    event.preventDefault();
    this.error.hidden = true;
    this.submit.disabled = true;
    try {
      const body = {
        name: this.name.value.trim(),
        queue: this.queue.value.trim(),
        payload: parseObject(this.payload.value, "The payload"),
        metadata: parseObject(this.metadata.value, "The metadata"),
        priority: number(this.priority),
        delay_seconds: number(this.delay),
        max_attempts: number(this.maxAttempts),
        timeout_seconds: number(this.timeout),
        dedupe_key: this.dedupe.value.trim() || undefined,
      };
      const task = await api("/tasks", { method: "POST", body });
      const note = h("span", null, task.created ? "Enqueued " : "Already queued: ", h("a", { href: `${root}/ui/tasks/${task.id}` }, task.name));
      toast(note);
      this.dispatchEvent(new CustomEvent("enqueued", { detail: task }));
      this.dialog.close();
    } catch (error) {
      this.error.textContent = error.message;
      this.error.hidden = false;
    } finally {
      this.submit.disabled = false;
    }
  }
}

customElements.define("tq-enqueue", Enqueue);
