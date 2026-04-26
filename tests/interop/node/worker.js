// BullMQ TS Worker that sits on the queue specified by env vars and
// processes whatever lands. Used by the Go interop tests for the
// "mkq Add -> BullMQ TS Process" direction.
//
// Env:
//   INTEROP_REDIS    e.g. 127.0.0.1:6379  (host:port)
//   INTEROP_PREFIX   BullMQ keyPrefix (e.g. mkqtest-foo)
//   INTEROP_QUEUE    queue name (e.g. deliver)
//
// Behaviour:
//   - Echoes the input payload as the return value, plus a "by"
//     marker so the Go side can verify which side handled it.
//   - Logs a single line of JSON to stdout once the worker is
//     fully ready, so the Go side can wait deterministically:
//       {"event":"ready"}
//   - Exits cleanly on SIGTERM.

import { Worker } from "bullmq";

const redisAddr = process.env.INTEROP_REDIS;
const prefix    = process.env.INTEROP_PREFIX;
const queueName = process.env.INTEROP_QUEUE;

if (!redisAddr || !prefix || !queueName) {
  console.error("missing env: INTEROP_REDIS / INTEROP_PREFIX / INTEROP_QUEUE");
  process.exit(2);
}

const [host, portStr] = redisAddr.split(":");
const port = Number(portStr);

const worker = new Worker(
  queueName,
  async (job) => {
    return { by: "bullmq-ts", echo: job.data, jobId: job.id };
  },
  {
    connection: { host, port },
    prefix,
  }
);

worker.on("ready", () => {
  process.stdout.write(JSON.stringify({ event: "ready" }) + "\n");
});

const shutdown = async () => {
  try {
    await worker.close();
  } finally {
    process.exit(0);
  }
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
