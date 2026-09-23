// scheduler.js — drive BullMQ TS's own upsertJobScheduler so Go tests
// can compare what it writes against what mkq writes.
//
// Usage:
//   scheduler.js upsert <scheduleId> <everyMs> '<templateOptsJSON>'
//
// The template opts JSON is passed to upsertJobScheduler as the job
// template (`opts`), which is where BullMQ puts removeOnComplete /
// removeOnFail for scheduled jobs.
//
// Env: INTEROP_REDIS / INTEROP_PREFIX / INTEROP_QUEUE
// Output: single JSON line on stdout. Non-zero exit on error.

import { Queue } from "bullmq";

const redisAddr = process.env.INTEROP_REDIS;
const prefix = process.env.INTEROP_PREFIX;
const queueName = process.env.INTEROP_QUEUE;
const [sub, ...args] = process.argv.slice(2);

if (!redisAddr || !prefix || !queueName || !sub) {
  console.error("usage: scheduler.js <sub> [args...]; needs INTEROP_REDIS / PREFIX / QUEUE env");
  process.exit(2);
}

const [host, portStr] = redisAddr.split(":");
const queue = new Queue(queueName, {
  connection: { host, port: Number(portStr) },
  prefix,
});

try {
  let result;
  switch (sub) {
    case "upsert": {
      const [scheduleId, everyStr, optsJSON] = args;
      const job = await queue.upsertJobScheduler(
        scheduleId,
        { every: Number(everyStr) },
        { name: queueName, data: {}, opts: JSON.parse(optsJSON ?? "{}") },
      );
      result = { jobId: job ? job.id : null };
      break;
    }
    default:
      console.error(`unknown subcommand: ${sub}`);
      process.exit(2);
  }
  process.stdout.write(JSON.stringify(result) + "\n");
} finally {
  await queue.close();
}
