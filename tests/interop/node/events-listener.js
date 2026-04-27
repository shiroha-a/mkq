// events-listener.js — subscribes to BullMQ TS QueueEvents on the
// target queue and prints each event as a JSON line to stdout. Used
// by the Go interop tests to verify that mkq-emitted events on the
// Redis Stream are consumable by a BullMQ TS reader (= mkq's wire
// format for the events stream is correct in the inverse direction).
//
// Env: INTEROP_REDIS / INTEROP_PREFIX / INTEROP_QUEUE
// Optional INTEROP_EVENTS: comma-separated list of event names to
// emit; defaults to all (added/waiting/active/completed/failed/
// delayed/stalled/drained/progress/removed/retries-exhausted).
//
// Output (per event):
//   {"event":"ready"}                 once subscribed
//   {"event":"<name>","jobId":...,...} per emitted event
//
// Exits cleanly on SIGTERM.

import { QueueEvents } from "bullmq";

const redisAddr = process.env.INTEROP_REDIS;
const prefix    = process.env.INTEROP_PREFIX;
const queueName = process.env.INTEROP_QUEUE;
const eventsArg = process.env.INTEROP_EVENTS;

if (!redisAddr || !prefix || !queueName) {
  console.error("missing env: INTEROP_REDIS / INTEROP_PREFIX / INTEROP_QUEUE");
  process.exit(2);
}

const [host, portStr] = redisAddr.split(":");
const wanted = eventsArg
  ? new Set(eventsArg.split(","))
  : null;

const qe = new QueueEvents(queueName, {
  connection: { host, port: Number(portStr) },
  prefix,
});

const emit = (name, payload) => {
  if (wanted && !wanted.has(name)) return;
  process.stdout.write(JSON.stringify({ event: name, ...payload }) + "\n");
};

qe.on("waiting",          (data) => emit("waiting",          data));
qe.on("active",           (data) => emit("active",           data));
qe.on("completed",        (data) => emit("completed",        data));
qe.on("failed",           (data) => emit("failed",           data));
qe.on("delayed",          (data) => emit("delayed",          data));
qe.on("stalled",          (data) => emit("stalled",          data));
qe.on("drained",          ()      => emit("drained",         {}));
qe.on("progress",         (data) => emit("progress",         data));
qe.on("removed",          (data) => emit("removed",          data));
qe.on("retries-exhausted",(data) => emit("retries-exhausted",data));
qe.on("added",            (data) => emit("added",            data));

await qe.waitUntilReady();
process.stdout.write(JSON.stringify({ event: "ready" }) + "\n");

const shutdown = async () => {
  try {
    await qe.close();
  } finally {
    process.exit(0);
  }
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
