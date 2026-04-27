# Changelog

All notable changes to mkq are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-04-27

Initial stable release. Wire format and public API are stable from
this point onward; subsequent releases will only add features and
fix bugs without breaking existing callers.

### Added

#### Core (Phase 3)

- BullMQ-compatible `Queue.Add` with the three vendored entry-point
  Lua scripts (`addStandardJob-9` / `addDelayedJob-6` /
  `addPrioritizedJob-9`).
- Worker happy-path via `mkq.Process` — generic over the payload type
  with `Handler[T]`, lock heartbeat, panic recovery, graceful
  `Worker.Stop`.
- Retry and backoff: `WithAttempts`, `WithBackoff` with `FixedBackoff`
  / `ExponentialBackoff` constructors, `ErrUnrecoverable` sentinel.
- Job options: `WithDelay`, `WithPriority`, `WithLifo`, `WithJobID`.
- Retention controls: `WithKeepCompleted` / `WithKeepFailed` (count)
  and `WithKeepCompletedAge` / `WithKeepFailedAge` (age).
- Stalled-job detection (`moveStalledJobsToWait`) with
  `WithStalledInterval` / `WithMaxStalledCount`.
- Rate limiter: `WithRateLimit(max, duration)` honoured at
  `moveToActive` time; cross-worker via the BullMQ rate-limit ZSET.

#### BullMQ compat layer (Phase 4)

- Cross-language interop harness (Go test orchestrating Node + the
  real BullMQ TS library) — 29 subtests covering producer / consumer
  / mutation / admin / retry / stalled recovery / shared rate limit
  / inverse events / schedule options / dedup.
- bull-board admin-UI smoke test confirming queues populated by mkq
  render correctly through the BullMQ TS reader.
- `Queue.Get` + `JobState` for post-finalisation HASH read-side
  parity (returnvalue, processedOn, finishedOn, failedReason,
  stacktrace, attemptsMade, attemptsStarted, stalledCounter,
  processedBy).
- `QueueEvents.Subscribe` — typed subscriber for the BullMQ events
  stream (added / waiting / active / completed / failed / progress /
  stalled / drained / delayed / retries-exhausted / removed).
- Repeat scheduler — `Queue.UpsertScheduleEvery` (every-mode) and
  `Queue.UpsertSchedulePattern` (cron-pattern mode using the
  robfig/cron/v3 5-field Vixie parser); `Queue.RemoveSchedule`.
  Schedule options: `WithScheduleLimit`, `WithScheduleStartDate`,
  `WithScheduleEndDate`, `WithScheduleTimezone`,
  `WithScheduleImmediately`.
- Job mutation API: `Job.UpdateProgress` / `Job.UpdateData` /
  `Job.Log`, plus the out-of-band variants on `Queue`
  (`UpdateJobProgress` / `UpdateJobData` / `AppendJobLog`).
- Deduplication: `WithDeduplication(id, ttl)` and the
  asynq-compatible `WithUnique(id, ttl)` alias.

#### mk-go integration prereqs (Phase 5)

- Inspector API (read-only): `Client.Queues`, `Queue.Counts`,
  `Queue.ListJobs`, `Queue.Get`.
- Inspector API (mutations): `Queue.RemoveJob`, `Queue.DrainPending`
  (with `WithDrainDelayed`), `Queue.PromoteJob`, `Queue.RetryJob`.
  Five typed errors: `ErrJobNotFound`, `ErrJobActive`,
  `ErrJobIsScheduler`, `ErrJobNotInDelayed`,
  `ErrJobNotInExpectedState`.
- `WithJobName(string)` — per-task-type fan-out within a single
  queue.

#### 1.0 release prep (Phase 6)

- Performance: marker-based BZPopMin dispatch (replaces polling +
  fixed sleep), `moveToFinished` `fetchNext=true` (halves dispatch
  EVALSHAs), `sync.Pool` for msgpack encoders on the hot path.
- Benchmark harness vs BullMQ TS in `bench/` (Go + Node clients
  sharing the same Redis); end-to-end consume / produce / latency
  / memory metrics. mkq beats BullMQ TS on consume at
  concurrency ≥ 64; p99 latency mkq-favored 1.33–1.48× across all
  configurations.
- Observability: `Logger` / `Metrics` / `Tracer` / `Span` interfaces
  defaulting to noop; opt-in adapters in `observability/slogadapter`,
  `observability/promadapter`, `observability/oteladapter`. Five
  metrics and two spans emitted; five previously-silent operational
  sites (stalled-scan failures, NOSCRIPT reload, fetchNext shutdown
  race, BZPopMin pool-size warning, cron pattern parse failure)
  now surface through Logger.
- README, asynq migration guide
  (`docs/MIGRATING_FROM_ASYNQ.md`), godoc audit.

### Compatibility caveats

- Cluster mode: tested against go-redis cluster clients;
  cross-slot operations are avoided as in BullMQ.
- Pool sizing: BZPopMin holds a connection per worker slot. Set
  `Config.Redis.PoolSize` to at least `concurrency + 8`. mkq warns
  at startup when the configured pool is below this.
- Redis 6.0 and earlier: untested. The vendored Lua scripts use
  Redis 7-shaped command paths in places.

### Known limitations

- BullMQ flow producer (parent-child orchestration) — not
  implemented; the BullMQ TS feature is rarely needed in mk-go's
  workload and would touch wire-format territory we have not
  covered with interop tests yet.
- Sandboxed processors — not implemented; mkq runs handlers in the
  caller's goroutines.
- Pause / resume per-queue — not implemented; the underlying
  `paused` ZSET wire format is honoured by readers but mkq has no
  Go-side toggle yet.
- ioredis auto-pipelining behaviour cannot be matched by go-redis
  v9 (per-call EVALSHA), so 100k-job consume workloads see BullMQ
  TS pull ahead 1.24× at concurrency=16. Documented as the
  Redis-client-level gap in `bench/README.md`.

[Unreleased]: https://github.com/shiroha-a/mkq/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.0
