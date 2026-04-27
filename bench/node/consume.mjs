// consume.mjs — process N pre-enqueued jobs with a no-op handler.
//
// Pair with bench/go/main.go -mode=consume. The producer side is run
// separately first so the consumer doesn't bottleneck on enqueue rate.
//
// Output: a single JSON line on stdout that bench/run.sh consumes.

import { Worker } from 'bullmq';

const args = parseArgs(process.argv.slice(2));
const jobs = parseInt(args.jobs ?? '1000', 10);
const concurrency = parseInt(args.concurrency ?? '16', 10);
const queueName = args.queue ?? 'bench';
const prefix = args.prefix ?? 'bullbench';
const [host, portStr] = (args.redis ?? '127.0.0.1:6379').split(':');

let processed = 0;
let resolveDone;
const done = new Promise((r) => { resolveDone = r; });

const cpuStart = process.cpuUsage();
const start = process.hrtime.bigint();

const worker = new Worker(
  queueName,
  async () => {
    processed += 1;
    if (processed === jobs) resolveDone();
  },
  {
    connection: { host, port: Number(portStr) },
    prefix,
    concurrency,
  },
);

await done;

const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
const cpu = process.cpuUsage(cpuStart);
const mem = process.memoryUsage();

await worker.close();

emit({
  side: 'bullmq-ts',
  mode: 'consume',
  jobs,
  concurrency,
  ms: Math.round(elapsedMs),
  perSec: jobs / (elapsedMs / 1000),
  rssBytes: mem.rss,
  heapUsedBytes: mem.heapUsed,
  userMicros: cpu.user,
  systemMicros: cpu.system,
});

function parseArgs(argv) {
  const out = {};
  for (const a of argv) {
    const [k, v] = a.replace(/^--/, '').split('=');
    out[k] = v ?? 'true';
  }
  return out;
}

function emit(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}
