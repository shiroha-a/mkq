// inspector.js — read-only BullMQ TS Queue inspections used by Go
// interop tests to assert state mkq produced.
//
// Subcommands (first argv after the script):
//   getJob <jobId>             — Job.fromId; emits the full Job JSON
//   getState <jobId>           — Job.getState; emits {state}
//   counts <s1> [s2 ...]       — Queue.getJobCounts(states...); emits {counts}
//   getJobs <state> <start> <end> — Queue.getJobs([state], start, end); emits {ids}
//   getJobLogs <jobId>         — Queue.getJobLogs(jobId); emits {logs, count}
//
// Env: INTEROP_REDIS / INTEROP_PREFIX / INTEROP_QUEUE
// Output: single JSON line on stdout. Non-zero exit on error.

import { Queue } from "bullmq";

const redisAddr = process.env.INTEROP_REDIS;
const prefix    = process.env.INTEROP_PREFIX;
const queueName = process.env.INTEROP_QUEUE;
const [sub, ...args] = process.argv.slice(2);

if (!redisAddr || !prefix || !queueName || !sub) {
  console.error("usage: inspector.js <sub> [args...]; needs INTEROP_REDIS / PREFIX / QUEUE env");
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
    case "getJob": {
      const job = await queue.getJob(args[0]);
      result = job ? job.toJSON() : null;
      break;
    }
    case "getState": {
      const job = await queue.getJob(args[0]);
      result = { state: job ? await job.getState() : "missing" };
      break;
    }
    case "counts": {
      result = { counts: await queue.getJobCounts(...args) };
      break;
    }
    case "getJobs": {
      const [state, startStr, endStr] = args;
      const start = parseInt(startStr ?? "0", 10);
      const end   = parseInt(endStr ?? "-1", 10);
      const jobs = await queue.getJobs([state], start, end);
      result = { ids: jobs.map((j) => j.id) };
      break;
    }
    case "getJobLogs": {
      result = await queue.getJobLogs(args[0]);
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
