// produce.mjs — push N jobs into a BullMQ queue as fast as possible.
//
// Pair with bench/go/main.go -mode=produce for an apples-to-apples
// throughput comparison. Both sides use the same payload shape, same
// Redis, same prefix discipline (each side picks its own prefix to
// avoid cross-test interference).
//
// Output: a single JSON line on stdout that bench/run.sh consumes.

import { Queue } from 'bullmq';

const args = parseArgs(process.argv.slice(2));
const jobs = parseInt(args.jobs ?? '1000', 10);
const queueName = args.queue ?? 'bench';
const prefix = args.prefix ?? 'bullbench';
const [host, portStr] = (args.redis ?? '127.0.0.1:6379').split(':');

const queue = new Queue(queueName, {
  connection: { host, port: Number(portStr) },
  prefix,
});

const payload = { inbox: 'https://example.org/inbox', body: 'hello' };

const start = process.hrtime.bigint();
const cpuStart = process.cpuUsage();

for (let i = 0; i < jobs; i++) {
  await queue.add('bench', payload);
}

const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
const cpu = process.cpuUsage(cpuStart);
const mem = process.memoryUsage();

await queue.close();

emit({
  side: 'bullmq-ts',
  mode: 'produce',
  jobs,
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
