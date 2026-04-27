// BullMQ TS Queue.add producer used by the Go interop tests for the
// "BullMQ TS Add -> mkq Process" direction. Adds one job and exits.
//
// Env:
//   INTEROP_REDIS     e.g. 127.0.0.1:6379
//   INTEROP_PREFIX    BullMQ keyPrefix
//   INTEROP_QUEUE     queue name
//   INTEROP_PAYLOAD   JSON-encoded job data (default: {"hello":"from-bullmq"})
//   INTEROP_NAME      Job.name override (default: queue name)
//   INTEROP_OPTS      JSON-encoded BullMQ JobsOptions (delay / priority /
//                     attempts / removeOnComplete / removeOnFail /
//                     deduplication / lifo / etc.). Forwarded verbatim.
//
// Output:
//   single line of JSON on stdout: {"jobId":"<id>"}
//   non-zero exit on failure.

import { Queue } from "bullmq";

const redisAddr = process.env.INTEROP_REDIS;
const prefix    = process.env.INTEROP_PREFIX;
const queueName = process.env.INTEROP_QUEUE;
const payload   = process.env.INTEROP_PAYLOAD || JSON.stringify({ hello: "from-bullmq" });
const jobName   = process.env.INTEROP_NAME || queueName;
const jobOpts   = process.env.INTEROP_OPTS ? JSON.parse(process.env.INTEROP_OPTS) : undefined;

if (!redisAddr || !prefix || !queueName) {
  console.error("missing env: INTEROP_REDIS / INTEROP_PREFIX / INTEROP_QUEUE");
  process.exit(2);
}

const [host, portStr] = redisAddr.split(":");
const port = Number(portStr);

const queue = new Queue(queueName, {
  connection: { host, port },
  prefix,
});

try {
  const job = await queue.add(jobName, JSON.parse(payload), jobOpts);
  process.stdout.write(JSON.stringify({ jobId: job.id }) + "\n");
} finally {
  await queue.close();
}
