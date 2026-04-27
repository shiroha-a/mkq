# mkq vs BullMQ TS — benchmark harness

A small comparison rig that drives both libraries through the same
workload against a single Redis and reports throughput, latency, peak
RSS, and CPU time.

## Reproduction

Prerequisites:
- Redis on `127.0.0.1:6379` (override via `REDIS=host:port`)
- Go 1.25+ (matches the repo's `go.mod` toolchain directive)
- Node.js 22+
- GNU `time` (Debian/Ubuntu: `apt install time`; macOS: `brew install gnu-time` and the script becomes `gtime`)

Then:

```bash
# 1k jobs, smoke
bench/run.sh smoke

# 10k jobs, the standard run
bench/run.sh standard

# custom job count
bench/run.sh 50000
```

Each mode runs twice — once mkq, once BullMQ TS — with distinct key
prefixes (`mkqbench:` vs `bullbench:`) so the same Redis instance can
host both without cross-contamination. The script wipes both prefixes
before and after.

## Methodology

Each side does:

| Mode      | What it measures                                          |
|-----------|-----------------------------------------------------------|
| `produce` | Add N jobs as fast as possible. Wall time + jobs/sec.     |
| `consume` | Pre-enqueued N jobs drained by a worker (concurrency=16). |
| `latency` | Add N jobs, then drain. Per-job e2e p50 / p99 in ms.      |

Workload:

- Payload: `{"inbox":"https://example.org/inbox","body":"hello"}` (~50 bytes)
- Job count: 10,000 (standard run)
- Worker concurrency: 16
- Handler: no-op (`return nil`)

Application metrics are emitted as a one-line JSON from each bench
program; OS-level RSS / CPU is captured by wrapping each invocation
with `/usr/bin/time -v` and parsing the report. Both views are useful:
the in-process view excludes Redis client overhead the OS sees, while
the OS view includes runtime + GC behaviour the application doesn't
necessarily count toward its own working set.

## Results

Hardware: WSL2 on a developer laptop (Linux 6.8, Go 1.25.0, Node 22,
Redis 7-alpine, single-threaded Redis on the same host as the bench).
Numbers are from one representative run of `bench/run.sh standard`;
3 sequential runs vary by < 5% per metric.

### Producer (Add 10k jobs)

| Metric              |       mkq | BullMQ TS | Ratio              |
|---------------------|----------:|----------:|--------------------|
| jobs / sec          |    14,193 |     9,602 | mkq **1.5× faster** |
| wall time           |     704ms |   1,041ms |                    |
| user CPU            |     0.18s |     0.86s | mkq 4.8× less      |
| peak RSS            |   13.7 MB |  101.6 MB | mkq 7.4× smaller   |
| heap allocated/job  |     2.1 KB | (not exposed) |                |

### Consumer (drain 10k jobs, concurrency=16)

| Metric              |       mkq | BullMQ TS | Ratio              |
|---------------------|----------:|----------:|--------------------|
| jobs / sec          |    10,203 |    15,256 | BullMQ **1.5× faster** |
| wall time           |     980ms |     655ms |                    |
| user CPU            |     0.66s |     0.88s | mkq 1.3× less      |
| peak RSS            |   15.4 MB |  110.7 MB | mkq 7.2× smaller   |
| heap allocated/job  |     6.8 KB | (not exposed) |                |

The consumer-side gap is the polling-vs-blocking trade-off: mkq
dispatches via the polling loop with `WithIdlePollInterval(10ms)` to
keep ctx cancellation responsive, while BullMQ uses `BLPOP` which
wakes the moment a job lands in the wait list. Lowering mkq's idle
interval narrows the gap at the cost of hotter idle Redis traffic.
This is a known design point — see #1 ("Open questions").

### Drain-latency (10k pre-enqueued jobs, concurrency=16)

p50 / p99 here measure the per-job Add → handler-entry delta over the
full drain. Because all jobs are enqueued before the worker starts,
p99 ≈ wall time of the drain (the last job had to wait for the queue
to empty in front of it). It is not a single-job dispatch latency
measurement.

| Metric    |    mkq | BullMQ TS |
|-----------|-------:|----------:|
| p50       |  869ms |     825ms |
| p99       |  993ms |   1,074ms |
| user CPU  |  0.86s |     1.52s |
| peak RSS  | 17.4 MB | 110.0 MB |

Tail (p99) is tighter on mkq: the polling cadence smooths the dispatch
distribution where BullMQ's BLPOP can spike on the last few jobs.

## Interpretation

Where mkq wins, why:
- **Memory** (~7× smaller RSS): no Node.js runtime / V8 heap baseline.
- **Producer throughput** (1.5× faster): fewer round-trips per Add — the vendored Lua + cached SHA path skips BullMQ TS's per-call command-tree traversal.
- **CPU per job** (1.3–4.8× less user CPU): native code vs JIT.
- **p99 drain tail** (~8% tighter): polling smooths dispatch order.

Where BullMQ TS wins, why:
- **Consumer throughput** (1.5× faster): `BLPOP` wakes immediately, mkq's 10 ms poll interval delays each dispatch. Acceptable trade-off for predictable ctx cancellation and idle Redis pressure; tunable via `WithIdlePollInterval`.

Both numbers are within the same order of magnitude — neither library
is a dramatic outlier. Pick on integration / API ergonomics first, on
memory budget if you're packing many workers per host.

## CI

This harness is intentionally **not** wired into CI. Numbers are
sensitive to host load, Redis tick rate, and kernel scheduler;
publishing CI-noisy results would mislead. Run it locally on the
target hardware before drawing conclusions.
