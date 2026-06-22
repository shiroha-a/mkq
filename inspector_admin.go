package mkq

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/shiroha-a/mkq/internal/lua"
)

// RemoveJob deletes a job and all its associated keys (logs, lock,
// dependency markers) from any state set it currently lives in.
// Equivalent to BullMQ's Job.remove.
//
// Returns:
//   - ErrJobActive       — job is locked (in active state); the worker
//     must finish or release the lock first.
//   - ErrJobIsScheduler  — job is the current iteration of a recurring
//     schedule; use RemoveSchedule on the parent
//     schedule instead of removing one fire.
//   - nil on success, including when the job had already been removed
//     by another path (BullMQ's removeJob is idempotent at the wire
//     level — it no-ops on missing HASHes rather than erroring).
func (q *Queue[T]) RemoveJob(ctx context.Context, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.RemoveJob,
		[]string{q.keys.Job(jobID), q.keys.Repeat()},
		jobID,
		"0", // shouldRemoveChildren=false (parent-child orchestration is out of mkq's scope today)
		q.keys.Base(),
	)
	if err != nil {
		return fmt.Errorf("mkq: removeJob: %w", err)
	}
	code, ok := res.(int64)
	if !ok {
		return fmt.Errorf("mkq: removeJob: unexpected result type %T", res)
	}
	switch code {
	case 1, 0:
		// 1 = removed; 0 = locked (active). The lua returns 0 when
		// isLocked succeeds; mkq surfaces it as ErrJobActive.
		if code == 0 {
			return ErrJobActive
		}
		return nil
	case -8:
		return ErrJobIsScheduler
	default:
		return fmt.Errorf("mkq: removeJob returned error code %d", code)
	}
}

// DrainOption customises a DrainPending call.
type DrainOption func(*drainConfig)

type drainConfig struct {
	includeDelayed bool
}

// WithDrainDelayed extends DrainPending to also remove jobs from the
// delayed ZSET. Default false: only wait / paused / prioritized are
// drained. Scheduler-owned delayed jobs (rjk-stamped) are preserved
// regardless — drain-5.lua's preflight skips any delayed job whose ID
// matches a current scheduler iteration.
func WithDrainDelayed(b bool) DrainOption {
	return func(c *drainConfig) { c.includeDelayed = b }
}

// DrainPending removes every queued job that hasn't been picked up
// yet. Active jobs (in flight under a worker lock) and terminal
// jobs (completed / failed) are left alone — DrainPending is the
// administrative "cancel everything pending" knob, not a wipe.
//
// Buckets drained:
//   - wait, paused (LIST-backed)
//   - prioritized (ZSET-backed)
//   - delayed (ZSET-backed) only when WithDrainDelayed(true) is set
//
// Scheduler iterations whose IDs match an entry in the repeat ZSET
// are preserved even when WithDrainDelayed(true) is set; the lua
// computes the protected set from the repeat ZSET before draining.
func (q *Queue[T]) DrainPending(ctx context.Context, opts ...DrainOption) error {
	cfg := drainConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	delayedFlag := "0"
	if cfg.includeDelayed {
		delayedFlag = "1"
	}

	_, err := q.client.scripts.Run(
		ctx,
		lua.Drain,
		[]string{
			q.keys.Wait(),
			q.keys.Paused(),
			q.keys.Delayed(),
			q.keys.Prioritized(),
			q.keys.Repeat(),
		},
		q.keys.Base(),
		delayedFlag,
	)
	// drain-5.lua はステータスコードを返さない。go-redis は何も返さない
	// EVALSHA を redis.Nil として surface するので、これは成功と扱う。
	// 実エラー (NOSCRIPT 後の reload 失敗等) はそのまま伝播。
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("mkq: drain: %w", err)
	}
	return nil
}

// PromoteJob moves a delayed job to the wait list immediately,
// bypassing its scheduled fire time. Equivalent to BullMQ's
// Job.promote and asynq's Inspector.RunTask for delayed jobs.
//
// Returns ErrJobNotInDelayed when the job is not in the delayed
// ZSET (already promoted, already finished, or never delayed). For
// retry-state jobs (failed / completed re-enqueue) use RetryJob.
func (q *Queue[T]) PromoteJob(ctx context.Context, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.Promote,
		[]string{
			q.keys.Delayed(),
			q.keys.Wait(),
			q.keys.Paused(),
			q.keys.Meta(),
			q.keys.Prioritized(),
			q.keys.Active(),
			q.keys.PriorityCounter(),
			q.keys.Events(),
			q.keys.Marker(),
		},
		q.keys.Base(),
		jobID,
	)
	if err != nil {
		return fmt.Errorf("mkq: promote: %w", err)
	}
	code, ok := res.(int64)
	if !ok {
		return fmt.Errorf("mkq: promote: unexpected result type %T", res)
	}
	switch code {
	case 0:
		return nil
	case -3:
		return ErrJobNotInDelayed
	default:
		return fmt.Errorf("mkq: promote returned error code %d", code)
	}
}

// RetryOption customises a RetryJob call.
type RetryOption func(*retryConfig)

type retryConfig struct {
	fromState JobBucket
	resetAtm  bool
	resetAts  bool
}

// WithRetryFromState picks the source bucket the job is currently in.
// Default JobBucketFailed; pass JobBucketCompleted for the rare case
// of re-enqueueing a successful job (matches BullMQ Job.retry's
// behaviour for both failed and completed sources).
func WithRetryFromState(state JobBucket) RetryOption {
	return func(c *retryConfig) { c.fromState = state }
}

// WithResetAttempts resets the BullMQ `atm` (attemptsMade) and `ats`
// (attemptsStarted) HASH counters before re-enqueueing. Default true
// (matches BullMQ's Job.retry default and asynq's RunTask).
func WithResetAttempts(reset bool) RetryOption {
	return func(c *retryConfig) {
		c.resetAtm = reset
		c.resetAts = reset
	}
}

// RetryJob re-enqueues a job from the failed (or completed) ZSET
// back into wait. Equivalent to BullMQ's Job.retry and the second
// half of asynq's Inspector.RunTask (the first half is PromoteJob
// for delayed jobs).
//
// Returns:
//   - ErrJobNotFound          — the job HASH is missing entirely
//     (already removed / retention-expired).
//   - ErrJobNotInExpectedState — the job exists but isn't in the
//     source bucket (e.g. RetryJob from
//     "failed" but the job is in "wait").
func (q *Queue[T]) RetryJob(ctx context.Context, jobID string, opts ...RetryOption) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	cfg := retryConfig{
		fromState: JobBucketFailed,
		resetAtm:  true,
		resetAts:  true,
	}
	for _, o := range opts {
		o(&cfg)
	}

	var stateKey, propVal, prevState string
	switch cfg.fromState {
	case JobBucketFailed:
		stateKey = q.keys.Failed()
		propVal = "failedReason"
		prevState = "failed"
	case JobBucketCompleted:
		stateKey = q.keys.Completed()
		propVal = "returnvalue"
		prevState = "completed"
	default:
		return fmt.Errorf("mkq: RetryJob source state %q must be JobBucketFailed or JobBucketCompleted", cfg.fromState)
	}

	resetAtm, resetAts := "0", "0"
	if cfg.resetAtm {
		resetAtm = "1"
	}
	if cfg.resetAts {
		resetAts = "1"
	}

	res, err := q.client.scripts.Run(
		ctx,
		lua.ReprocessJob,
		[]string{
			q.keys.Job(jobID),
			q.keys.Events(),
			stateKey,
			q.keys.Wait(),
			q.keys.Meta(),
			q.keys.Paused(),
			q.keys.Active(),
			q.keys.Marker(),
		},
		jobID,
		"LPUSH", // FIFO push (matches BullMQ default; LIFO retry isn't a documented BullMQ feature)
		propVal,
		prevState,
		resetAtm,
		resetAts,
	)
	if err != nil {
		return fmt.Errorf("mkq: reprocessJob: %w", err)
	}
	code, ok := res.(int64)
	if !ok {
		return fmt.Errorf("mkq: reprocessJob: unexpected result type %T", res)
	}
	switch code {
	case 1:
		return nil
	case -1:
		return ErrJobNotFound
	case -3:
		return ErrJobNotInExpectedState
	default:
		return fmt.Errorf("mkq: reprocessJob returned error code %d", code)
	}
}

// Pause stops the queue from handing jobs to workers, mirroring
// BullMQ's Queue.pause(). It atomically sets the `meta.paused` flag and
// renames the `wait` list onto `paused`, so any jobs already queued are
// parked rather than dropped. Jobs enqueued while paused also land in
// `paused` (addStandardJob honours the flag via getTargetQueueList), so
// nothing is orphaned; Resume returns the whole set to `wait`.
//
// The pause is global to the queue and shared via Redis, so every
// worker process bound to the same queue honours it (the gate lives in
// moveToActive's Lua, not in any single client). In-flight jobs already
// locked by a worker are allowed to finish — Pause only blocks new
// fetches.
//
// Equivalent to BullMQ pause-7.lua with ARGV "paused". Idempotent:
// pausing an already-paused queue is a no-op at the wire level.
func (q *Queue[T]) Pause(ctx context.Context) error {
	return q.pauseResume(ctx, q.keys.Wait(), q.keys.Paused(), "paused")
}

// Resume re-enables job processing on a paused queue, mirroring
// BullMQ's Queue.resume(). It clears the `meta.paused` flag, renames the
// `paused` list back onto `wait`, and pokes the marker ZSET so blocking
// workers wake immediately instead of waiting out their BRPOPLPUSH
// timeout.
//
// Equivalent to BullMQ pause-7.lua with ARGV "resumed". Idempotent:
// resuming a queue that is not paused is a no-op at the wire level.
func (q *Queue[T]) Resume(ctx context.Context) error {
	return q.pauseResume(ctx, q.keys.Paused(), q.keys.Wait(), "resumed")
}

// pauseResume runs the vendored pause-7.lua. source/target are the
// LIST keys to rename (wait->paused for pause, paused->wait for
// resume); event is the BullMQ stream event ("paused" or "resumed")
// that also selects the branch inside the Lua.
func (q *Queue[T]) pauseResume(ctx context.Context, source, target, event string) error {
	_, err := q.client.scripts.Run(
		ctx,
		lua.Pause,
		// pause-7.lua KEYS:
		//   1 source list, 2 target list, 3 meta, 4 prioritized,
		//   5 events, 6 delayed, 7 marker
		[]string{
			source,
			target,
			q.keys.Meta(),
			q.keys.Prioritized(),
			q.keys.Events(),
			q.keys.Delayed(),
			q.keys.Marker(),
		},
		event,
	)
	// pause-7.lua はステータスコードを返さない (末尾は XADD)。返り値の
	// 無い EVALSHA を go-redis は redis.Nil として surface するので成功
	// 扱い。実エラー (NOSCRIPT 後の reload 失敗等) はそのまま伝播。
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("mkq: %s: %w", event, err)
	}
	return nil
}

// IsPaused reports whether the queue is currently paused, mirroring
// BullMQ's Queue.isPaused(). It checks for the `paused` field on the
// `meta` HASH (HEXISTS) — an empty `paused` list and an unpaused queue
// are distinct states, so the flag, not the list, is authoritative.
func (q *Queue[T]) IsPaused(ctx context.Context) (bool, error) {
	paused, err := q.client.rdb.HExists(ctx, q.keys.Meta(), "paused").Result()
	if err != nil {
		return false, fmt.Errorf("mkq: isPaused: %w", err)
	}
	return paused, nil
}
