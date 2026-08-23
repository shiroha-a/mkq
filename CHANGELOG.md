# Changelog

All notable changes to mkq are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- `stacktrace` is now appended to on each failure instead of being
  overwritten with a single-element array. BullMQ TS accumulates one entry
  per attempt — that is why its consumers reverse the array to show the
  newest first. Overwriting meant **a retried job lost every earlier
  failure reason**, leaving only the last one.

### Added

- `JobState.AttemptsAt` records the start time of every failed attempt
  (unix milliseconds, oldest first), stored in the `mkqAttemptsAt` HASH
  field. **BullMQ has no equivalent** — it keeps no per-attempt timestamp,
  which is why admin UIs that try to plot retries have nothing to place
  them at. Unknown HASH fields are ignored by BullMQ and bull-board, so
  wire compatibility is preserved. Jobs that failed before this release do
  not have it and cannot be backfilled.

## [1.0.7] - 2026-08-24

### Added

- `JobState` now carries `Opts` (the BullMQ `opts` HASH field verbatim,
  as raw JSON) and `Delay` (milliseconds). `Queue.Get` already read the
  whole HASH and threw both away, so surfacing them costs no extra round
  trip. Admin UIs need `opts` to show a job's real `attempts` / `backoff`
  / `removeOnComplete` — reconstructing it from the fields mkq happens to
  know silently drops any key mkq does not model. `ListJobs` returns the
  same values.
- `Queue.GetJobLogs(ctx, jobID, start, end)` reads the log lines written
  by `Job.Log` / `Queue.AppendJobLog`, returning `JobLogs{Logs, Count}`.
  The write side existed since job mutations landed but there was no way
  to read them back, so an admin log tab had nothing to show. Mirrors
  BullMQ's `Queue.getJobLogs`: `count` is the full list length even when
  a sub-range is requested, and a missing job is indistinguishable from a
  job with no logs (both empty).

## [1.0.6] - 2026-08-18

### Fixed

- `Worker.Stop` could block forever when another worker was still
  running on the same queue. Stop wakes parked dispatchers by pushing to
  a Redis key, but it pushed to the queue-level `marker` — which every
  worker on that queue blocks on — so a surviving worker's dispatcher
  could consume the wake-up meant for the stopping one. A mk-go
  production instance failed to boot because of this: its autoscaler
  resized `inbox` from 16 workers down to 4, `Worker.Stop` never
  returned (the caller passed `context.Background()`), and the HTTP
  listener was never reached. Each Worker now owns a private wake ZSET
  and blocks on it alongside the shared marker, so a push always reaches
  the intended worker. The key is named with a uuid (`workerName` is
  caller-supplied and can collide) and is deleted once every dispatcher
  has exited. Measured: 32s (hitting a 20s context deadline) before,
  4ms after. (#78)
  - This is not a 1.0.5 regression — pushing to the shared marker dates
    from 1.0.4. It stayed hidden because dispatchers holding a delayed
    job sat in a precise sleep rather than on the marker; 1.0.5 moved
    them onto the marker and exposed it.

## [1.0.5] - 2026-08-18

### Fixed

- The idle backoff added in 1.0.4 was defeated by a single delayed job.
  When `tryOnce` reported the next delayed job's timestamp, the
  dispatcher took a precise-sleep path instead of parking on the marker,
  and that sleep was capped at `idlePollInterval` (100ms by default). So
  one job scheduled an hour out was enough to keep every dispatcher
  issuing `moveToActive` ten times a second — `awaitMarker` was never
  reached, so the wait never grew. With 8 workers, an otherwise idle
  queue went from 19 commands/s to **549**. Long delays now park on the
  marker (capped at 30s): a newly enqueued job still wakes the worker
  immediately via the marker push, so there is nothing to gain from
  polling. Sub-second delays keep the precise sleep — go-redis rounds a
  `BZPOPMIN` timeout up to one second, which would overshoot a 50ms
  retry backoff. (#76)

## [1.0.4] - 2026-08-18

### Changed

- Minimum Go version raised to 1.26 (matches mk-go's `go.mod`). CI now
  reads the version from `go.mod` via `setup-go`'s `go-version-file`,
  so the workflow and the module cannot drift apart.
- `WithIdlePollInterval` now sets the *floor* of the idle wait rather
  than a fixed period: an idle dispatcher doubles its wait on every
  empty poll, capped at 30s, and snaps back to the floor as soon as it
  processes a job. Job pickup latency is unchanged — the wait ends on
  the marker push that `addJob` performs — so callers do not need to
  retune the option. (#74)

### Fixed

- Idle workers no longer poll Redis once per second per dispatcher.
  go-redis rounds a `BZPOPMIN` timeout up to whole seconds, so every
  dispatcher woke and issued a `tryOnce` each second even with an empty
  queue, scaling with worker count. A mk-go production instance was
  issuing 774 commands/s against an idle queue where the equivalent
  BullMQ deployment issued 21.5. With the backoff, 8 workers idling for
  20s drop from 1,284 commands to 388. (#74)
- `Worker.Stop` is no longer bounded by `idlePollInterval`. Cancelling
  the context does not abort an in-flight `BZPOPMIN` — go-redis derives
  the read deadline from the block timeout and does not interrupt a
  read already issued — so shutdown waited out the remaining interval
  (7.78s measured at `interval=8s`). `Stop` now pokes the marker key to
  wake blocked dispatchers, bringing shutdown to single-digit
  milliseconds regardless of the interval. Each poked member is named
  per dispatcher: the marker is a sorted set, so repeating one member
  name collapses to a single entry and wakes only one waiter. (#74)

## [1.0.3] - 2026-06-23

### Added

- Public pause/resume API for BullMQ `Queue.pause()` / `Queue.resume()`
  parity. (#70)
  - `Queue.Pause` sets the `meta.paused` flag and atomically moves the
    `wait` list to `paused` (vendored `pause-7.lua`), so jobs already
    queued are parked rather than dropped. Jobs enqueued while paused
    also land in `paused` (no orphans); the pause is shared via Redis so
    every worker process honours it.
  - `Queue.Resume` clears the flag, returns parked jobs to `wait`, and
    pokes the marker ZSET so blocking workers wake immediately.
  - `Queue.IsPaused` reports the current state via `HEXISTS meta paused`.

## [1.0.2] - 2026-06-01

### Added

- Custom and jittered retry backoff strategies for BullMQ parity. (#67)
  - `FixedBackoffWithJitter` / `ExponentialBackoffWithJitter` apply
    BullMQ's jitter fraction (`0..1`); the value is persisted in the
    `opts.backoff` wire shape (`{type, delay, jitter}`) so foreign
    BullMQ workers honour it.
  - `CustomBackoff()` + `Worker.WithBackoffStrategy` expose BullMQ's
    `settings.backoffStrategy` path: an arbitrary `func(attemptsMade int)
    time.Duration` owns the formula, cap, and jitter. This lets mk-go
    reproduce Misskey's `httpRelatedBackoff` (`(2^n-1)*base`, capped at
    8h, plus 0-20% jitter) drop-in. (#66)

### Fixed

- (none)

## [1.0.1] - 2026-04-27

### Added

- BullMQ-compatible per-queue metrics (#59, #60). Enables the
  previously-disabled write path inside the vendored
  `moveToFinished-14.lua` so per-minute completed/failed buckets
  land in the BullMQ-spec keys (`bull:<q>:metrics:<target>` HASH +
  `...:data` LIST). bull-board / Misskey admin / mk-go admin
  charts that LRANGE the BullMQ key now see real data. Adopts
  BullMQ TS's API shape exactly:
  - `WithJobMetrics(maxDataPoints int) WorkerOption` — opt-in
    write path; mirrors BullMQ TS `WorkerOptions.metrics:
    { maxDataPoints }`.
  - `Queue[T].GetMetrics(ctx, kind, start, end) (QueueMetrics, error)`
    — atomic read via vendored `getMetrics-2.lua`.
  - `QueueMetrics{Meta: QueueMetricsMeta{Count, PrevTS, PrevCount},
    Data, Count}` matches BullMQ TS `Metrics` interface.
  - `ErrInvalidMetricsBucket` for non-completed/failed kinds.

### Notes

- This release is a strict additive minor under semver (no
  breaking changes); kept on the patch line per the project's
  early-stage `1.0.x` cadence.

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

[Unreleased]: https://github.com/shiroha-a/mkq/compare/v1.0.6...HEAD
[1.0.6]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.6
[1.0.5]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.5
[1.0.4]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.4
[1.0.3]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.3
[1.0.2]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.2
[1.0.1]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.1
[1.0.0]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.0
