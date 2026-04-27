// BullMQ TS Worker that sits on the queue specified by env vars and
// processes whatever lands. Used by the Go interop tests for the
// "mkq Add -> BullMQ TS Process" direction.
//
// Env:
//   INTEROP_REDIS         e.g. 127.0.0.1:6379  (host:port)
//   INTEROP_PREFIX        BullMQ keyPrefix (e.g. mkqtest-foo)
//   INTEROP_QUEUE         queue name (e.g. deliver)
//   INTEROP_FAIL          if "1", handler always throws (for retry interop)
//   INTEROP_CONCURRENCY   integer concurrency (default 1)
//   INTEROP_LIMITER       JSON {"max": N, "duration": ms} (optional)
//   INTEROP_LOCK_DURATION_MS  worker.lockDuration override (optional ms)
//   INTEROP_HOLD_MS       handler sleep before returning (default 0). Used
//                         to keep a job locked for stalled / lock interop.
//
// Behaviour:
//   - Default handler echoes the payload + a "by" marker.
//   - INTEROP_FAIL throws an Error with a fixed message so the
//     dispatcher records `failedReason`.
//   - Logs a single line of JSON to stdout once ready:
//       {"event":"ready"}
//   - For each processed job (success OR failure), writes:
//       {"event":"processed","jobId":"<id>","ok":true|false}
//   - Exits cleanly on SIGTERM.

import { Worker } from "bullmq";

const redisAddr        = process.env.INTEROP_REDIS;
const prefix           = process.env.INTEROP_PREFIX;
const queueName        = process.env.INTEROP_QUEUE;
const fail             = process.env.INTEROP_FAIL === "1";
const concurrency      = parseInt(process.env.INTEROP_CONCURRENCY ?? "1", 10);
const limiter          = process.env.INTEROP_LIMITER ? JSON.parse(process.env.INTEROP_LIMITER) : undefined;
const lockDurationOpt  = process.env.INTEROP_LOCK_DURATION_MS;
const lockDuration     = lockDurationOpt ? parseInt(lockDurationOpt, 10) : undefined;
const holdMs           = parseInt(process.env.INTEROP_HOLD_MS ?? "0", 10);

if (!redisAddr || !prefix || !queueName) {
  console.error("missing env: INTEROP_REDIS / INTEROP_PREFIX / INTEROP_QUEUE");
  process.exit(2);
}

const [host, portStr] = redisAddr.split(":");
const port = Number(portStr);

const workerOpts = {
  connection: { host, port },
  prefix,
  concurrency,
};
if (limiter) workerOpts.limiter = limiter;
if (lockDuration) workerOpts.lockDuration = lockDuration;

const worker = new Worker(
  queueName,
  async (job) => {
    if (holdMs > 0) {
      await new Promise((r) => setTimeout(r, holdMs));
    }
    if (fail) {
      throw new Error(`bullmq-ts handler intentional failure for job ${job.id}`);
    }
    return { by: "bullmq-ts", echo: job.data, jobId: job.id };
  },
  workerOpts
);

worker.on("ready", () => {
  process.stdout.write(JSON.stringify({ event: "ready" }) + "\n");
});
worker.on("completed", (job) => {
  process.stdout.write(JSON.stringify({ event: "processed", jobId: job.id, ok: true }) + "\n");
});
worker.on("failed", (job, err) => {
  process.stdout.write(JSON.stringify({ event: "processed", jobId: job?.id, ok: false, err: err?.message }) + "\n");
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
