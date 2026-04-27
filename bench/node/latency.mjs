// latency.mjs — measure end-to-end Add → process latency for N jobs.
//
// Mirrors bench/go/main.go -mode=latency: pre-enqueue every job
// (recording the wall-clock at Add), then start a Worker; the handler
// timestamps each job and we report p50 / p99 over the per-job deltas.

import { Queue, Worker } from 'bullmq';

const args = parseArgs(process.argv.slice(2));
const jobs = parseInt(args.jobs ?? '1000', 10);
const concurrency = parseInt(args.concurrency ?? '16', 10);
const queueName = args.queue ?? 'bench';
const prefix = args.prefix ?? 'bullbench';
const [host, portStr] = (args.redis ?? '127.0.0.1:6379').split(':');

const conn = { host, port: Number(portStr) };
const queue = new Queue(queueName, { connection: conn, prefix });

const addedAt = new Map();
const payload = { inbox: 'https://example.org/inbox', body: 'hello' };

const cpuStart = process.cpuUsage();

for (let i = 0; i < jobs; i++) {
  const tAdd = Number(process.hrtime.bigint());
  const job = await queue.add('bench', payload);
  addedAt.set(job.id, tAdd);
}

const latencies = [];
let processed = 0;
let resolveDone;
const done = new Promise((r) => { resolveDone = r; });

const worker = new Worker(
  queueName,
  async (job) => {
    const t1 = Number(process.hrtime.bigint());
    const t0 = addedAt.get(job.id);
    if (t0 !== undefined) {
      latencies.push((t1 - t0) / 1e6); // ns → ms
    }
    processed += 1;
    if (processed === jobs) resolveDone();
  },
  { connection: conn, prefix, concurrency },
);

await done;

const cpu = process.cpuUsage(cpuStart);
const mem = process.memoryUsage();

latencies.sort((a, b) => a - b);
const p50 = latencies[Math.floor(latencies.length * 0.50)] ?? 0;
const p99 = latencies[Math.floor(latencies.length * 0.99)] ?? 0;

await worker.close();
await queue.close();

emit({
  side: 'bullmq-ts',
  mode: 'latency',
  jobs,
  concurrency,
  p50ms: Math.round(p50),
  p99ms: Math.round(p99),
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
