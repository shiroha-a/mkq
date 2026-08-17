# mkq

BullMQ-compatible Go-native job queue. Drop-in replacement for
[asynq](https://github.com/hibiken/asynq) with a Redis layout that
[bull-board](https://github.com/felixmosh/bull-board), Misskey admin
UIs, and BullMQ workers in any language can share without translation.

```go
import "github.com/shiroha-a/mkq"

queue := mkq.Define[Email](client, "email")
queue.Add(ctx, Email{To: "alice@example.com"}, mkq.WithAttempts(3))

mkq.Process(queue, func(ctx context.Context, job *mkq.Job[Email]) (any, error) {
    return nil, send(job.Data)
})
```

## Why mkq

- **Wire-compatible with BullMQ v5+.** Foreign workers, bull-board,
  and other-language tooling read mkq's queues as ordinary BullMQ
  queues — no shim, no translation. Cross-language interop is covered
  by 29 integration tests against the real BullMQ TS library.
- **Go-native API.** Generics for typed payloads, context-aware
  handlers, structured options. No mirroring of BullMQ's JS class
  hierarchy.
- **Engineered for throughput.** Marker-based BZPopMin dispatch,
  fetchNext prefetch, msgpack encoder pooling. Beats BullMQ TS on
  consume throughput at concurrency ≥ 64; competitive at lower
  concurrencies. p99 handler-to-completion latency is mkq-favored
  1.33–1.48× across configurations.
- **Observability-ready.** Logger / Metrics / Tracer interfaces with
  opt-in slog / Prometheus / OTel adapters. Default = noop, no
  third-party deps in core.

## Status

Pre-1.0. Wire format is stable. API surface is stable for everything
in this README; observability adapters and Inspector API may gain
methods (additive only) before the 1.0 tag. See [#1] for the
roadmap.

[#1]: https://github.com/shiroha-a/mkq/issues/1

## Install

```sh
go get github.com/shiroha-a/mkq
```

Requires Go 1.26+ and Redis 7+ (Redis 6.2+ also works; tested against
the official `redis:7-alpine` container).

## Quickstart

### Producer

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/shiroha-a/mkq"
)

type Email struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

func main() {
    ctx := context.Background()

    client, err := mkq.NewClient(ctx, mkq.Config{
        Redis: redis.UniversalOptions{Addrs: []string{"localhost:6379"}},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    queue := mkq.Define[Email](client, "email")

    // Standard add.
    _, err = queue.Add(ctx, Email{To: "alice@example.com", Subject: "hi"})

    // Delayed + retry-on-failure with exponential backoff.
    _, err = queue.Add(ctx,
        Email{To: "bob@example.com", Subject: "later"},
        mkq.WithDelay(5*time.Second),
        mkq.WithAttempts(5),
        mkq.WithBackoff(mkq.ExponentialBackoff(time.Second)),
    )

    // Idempotent enqueue (dedup hit returns ErrDuplicateJob).
    _, err = queue.Add(ctx,
        Email{To: "carol@example.com"},
        mkq.WithUnique("welcome:carol", time.Hour),
    )
    _ = err
}
```

### Consumer

```go
worker, err := mkq.Process(queue,
    func(ctx context.Context, job *mkq.Job[Email]) (any, error) {
        if err := send(job.Data); err != nil {
            return nil, err // counts toward WithAttempts; retries respect WithBackoff
        }
        return nil, nil
    },
    mkq.WithConcurrency(16),
    mkq.WithLockDuration(30*time.Second),
)
if err != nil {
    log.Fatal(err)
}
defer worker.Stop(context.Background())
```

### Recurring schedule

```go
// Every 5 minutes.
err := queue.UpsertScheduleEvery(ctx, "metrics-cron", 5*time.Minute, Email{})

// Cron pattern (5-field, Vixie syntax via robfig/cron/v3).
err = queue.UpsertSchedulePattern(ctx, "midnight-job", "0 0 * * *", Email{},
    mkq.WithScheduleTimezone("Asia/Tokyo"),
    mkq.WithScheduleLimit(30),
)
```

### Inspector

Read-only admin lookup (cross-compatible with bull-board's view of
the same queues):

```go
counts, _   := queue.Counts(ctx) // {Wait, Active, Completed, Failed, Delayed, ...}
listed, _   := queue.ListJobs(ctx, mkq.JobBucketWait, 0, 99, true) // ascending=true
job, st, _  := queue.Get(ctx, listed[0].Job.ID)
_ = st // post-finalisation snapshot (returnvalue, processedOn, etc.)
```

Mutations:

```go
queue.RetryJob(ctx, jobID)
queue.PromoteJob(ctx, jobID)
queue.RemoveJob(ctx, jobID)
queue.DrainPending(ctx, mkq.WithDrainDelayed(true))

queue.Pause(ctx)             // stop handing jobs to workers (wait -> paused)
queue.Resume(ctx)            // resume (paused -> wait, wakes blocking workers)
paused, _ := queue.IsPaused(ctx)
```

### Job mutation from inside a handler

```go
mkq.Process(queue, func(ctx context.Context, job *mkq.Job[Upload]) (any, error) {
    for i, chunk := range job.Data.Chunks {
        upload(chunk)
        _ = job.UpdateProgress(ctx, float64(i+1)/float64(len(job.Data.Chunks)))
        _ = job.Log(ctx, fmt.Sprintf("chunk %d done", i))
    }
    return nil, nil
})
```

## Observability

Three optional interfaces — Logger / Metrics / Tracer — default to
noop. Wire any combination via `Config`:

```go
import (
    "log/slog"

    "github.com/prometheus/client_golang/prometheus"
    "go.opentelemetry.io/otel"

    "github.com/shiroha-a/mkq"
    "github.com/shiroha-a/mkq/observability/oteladapter"
    "github.com/shiroha-a/mkq/observability/promadapter"
    "github.com/shiroha-a/mkq/observability/slogadapter"
)

client, _ := mkq.NewClient(ctx, mkq.Config{
    Redis:   redis.UniversalOptions{Addrs: []string{"localhost:6379"}},
    Logger:  slogadapter.New(slog.Default()),
    Metrics: promadapter.New(prometheus.DefaultRegisterer),
    Tracer:  oteladapter.New(otel.Tracer("mkq")),
})
```

The adapters live in dedicated sub-packages so importers of core mkq
do not pull in `prometheus/client_golang` or the OTel SDK unless they
opt in.

Metrics emitted (initial set):

| Name | Type | Labels |
|---|---|---|
| `mkq_jobs_added_total` | counter | queue, name |
| `mkq_jobs_processed_total` | counter | queue, name, status |
| `mkq_handler_duration_seconds` | histogram | queue, name |
| `mkq_dispatch_wait_seconds` | histogram | queue |
| `mkq_jobs_in_flight` | gauge | queue |

Spans:

- `mkq.queue.add` — around `Queue.Add`
- `mkq.worker.process` — parent of any user spans created inside the
  handler

## Cross-language interop with BullMQ

mkq's Redis layout is a strict subset of BullMQ v5+'s. A typical
mixed-runtime deployment looks like:

- **Go services** use mkq for producers and consumers.
- **Node services** keep using BullMQ TS for producers and consumers
  on the same queues (no flag, no migration).
- **Operators** point bull-board / Misskey admin / other dashboards
  at the same Redis instance.

mkq's interop test suite continuously validates the directions both
ways: BullMQ TS Add → mkq Process and mkq Add → BullMQ TS Process,
plus admin paths (Inspector mutations, QueueEvents subscriptions,
shared rate-limit windows, dedup, schedulers, retry/backoff,
stalled-job recovery).

See [docs/MIGRATING_FROM_ASYNQ.md](docs/MIGRATING_FROM_ASYNQ.md) for
the asynq → mkq migration walkthrough.

## Features

- **Job lifecycle**: Add / Process / retry-on-error / WithAttempts /
  WithBackoff (Fixed, Exponential, jitter, custom strategy) / panic
  recovery / ErrUnrecoverable.
- **Job options**: WithDelay, WithPriority, WithLifo,
  WithKeepCompleted/Failed (count + age), WithDeduplication / WithUnique,
  WithJobName, WithJobID.
- **Worker options**: WithConcurrency, WithLockDuration,
  WithStalledInterval, WithMaxStalledCount, WithIdlePollInterval,
  WithRateLimit, WithWorkerName, WithBackoffStrategy, WithJobMetrics.
- **Recurring schedules**: every-mode and cron-pattern mode, with
  WithScheduleLimit / StartDate / EndDate / Timezone / Immediately.
- **QueueEvents**: subscribe to BullMQ's `events` stream
  (added / active / completed / failed / progress / stalled / drained).
- **Inspector** (read): `Queue.Counts`, `Queue.ListJobs`,
  `Queue.Get`, `Client.Queues`.
- **Inspector** (admin): `Queue.RemoveJob`, `Queue.DrainPending`,
  `Queue.PromoteJob`, `Queue.RetryJob`, `Queue.Pause`, `Queue.Resume`,
  `Queue.IsPaused`.
- **Job mutation from handler**: `Job.UpdateProgress`,
  `Job.UpdateData`, `Job.Log`; out-of-band `Queue.UpdateJobProgress`,
  `Queue.UpdateJobData`, `Queue.AppendJobLog`.
- **Observability**: Logger / Metrics / Tracer interfaces, slog /
  Prometheus / OTel adapters.
- **Stalled detection**: cross-language compatible (mkq's scanner
  recovers BullMQ-orphaned jobs and vice versa).

## Compatibility caveats

- **Cluster mode**: tested against go-redis cluster clients;
  cross-slot operations are avoided as in BullMQ.
- **Pool sizing**: BZPopMin holds a connection per worker slot.
  Set `Config.Redis.PoolSize` to at least `concurrency + 8`. mkq
  warns at startup when the configured pool is below this.
- **Redis 6.0 and earlier**: untested. The vendored Lua scripts use
  Redis 7-shaped command paths in places.

## Documentation

- Package godoc: https://pkg.go.dev/github.com/shiroha-a/mkq
- Migration from asynq: [docs/MIGRATING_FROM_ASYNQ.md](docs/MIGRATING_FROM_ASYNQ.md)
- Authoritative design (mk-go repo):
  https://github.com/shiroha-a/mk/blob/develop/docs/design/mkq-design.md

## Development

- `go test ./...` (requires a local Redis on `127.0.0.1:6379` or
  `MKQ_TEST_REDIS_ADDR`).
- `go test -tags interop ./tests/interop/...` runs the cross-language
  test suite against a vendored BullMQ TS and Node 20+.
- `bench/run.sh` runs the throughput / latency / memory benchmark
  vs BullMQ TS.

Contributions: file an issue first, branch off `develop`, follow
`CLAUDE.md` for commit / PR conventions.

## License

mkq is released under the MIT License. See `THIRD_PARTY_NOTICES.md`
for the BullMQ Lua scripts vendored under the same terms.
