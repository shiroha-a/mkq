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
