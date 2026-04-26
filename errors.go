package mkq

import "errors"

// ErrPriorityWithDelay is returned by Queue.Add when both WithPriority
// and WithDelay are supplied. BullMQ stores the priority in the HASH
// but the dispatch is delayed-only, so the priority would not affect
// dequeue ordering — surfacing the conflict avoids a silent footgun.
//
// A future PR may add a combined dispatch (delayed + prioritized) that
// removes this restriction.
var ErrPriorityWithDelay = errors.New("mkq: WithPriority cannot be combined with WithDelay")

// ErrUnrecoverable, when wrapped into a handler's returned error,
// instructs the worker to skip the retry path even if WithAttempts
// would otherwise allow another attempt. The job transitions straight
// to failed.
//
// This mirrors BullMQ's UnrecoverableError class for foreign-language
// workers: bull-board reports the same final-failed shape regardless
// of whether the rejection came from a JS Worker or mkq.
//
//	return fmt.Errorf("invalid input: %w", mkq.ErrUnrecoverable)
var ErrUnrecoverable = errors.New("mkq: unrecoverable error (skip retry)")

// ErrJobNotFound is returned by Queue.Get and the mutation methods
// (UpdateProgress / UpdateData / Log) when the BullMQ HASH for the
// target jobID is missing — the job either never existed, was
// removed via WithKeepCompleted(0), or expired from a separate
// retention policy.
var ErrJobNotFound = errors.New("mkq: job not found")

// ErrJobDetached is returned by Job mutation methods when the Job
// was constructed without a queue back-pointer (e.g. zero-valued
// for tests). All Job values produced by the mkq API — Queue.Add,
// Queue.Get, and the handler-dispatch path — carry the back-pointer
// automatically.
var ErrJobDetached = errors.New("mkq: job has no queue back-pointer (constructed outside mkq?)")

// ErrJobActive is returned by Queue.RemoveJob when the target job
// is currently locked (in active state). BullMQ's removeJob-2.lua
// returns 0 in this case; mkq surfaces it as ErrJobActive so callers
// can retry after the in-flight handler finishes.
var ErrJobActive = errors.New("mkq: job is active (locked) and cannot be removed")

// ErrJobIsScheduler is returned by Queue.RemoveJob when the job is
// the current iteration of a recurring schedule (jobId begins with
// "repeat:"). Removing such a job individually would orphan the
// schedule; use Queue.RemoveSchedule for the whole schedule instead.
// BullMQ's removeJob-2.lua returns -8 in this case.
var ErrJobIsScheduler = errors.New("mkq: job is owned by a scheduler; use RemoveSchedule")

// ErrJobNotInDelayed is returned by Queue.PromoteJob when the
// target job is not currently in the delayed ZSET (already promoted,
// already finished, or never delayed). BullMQ's promote-9.lua
// returns -3 in this case.
var ErrJobNotInDelayed = errors.New("mkq: job is not in the delayed state")

// ErrJobNotInExpectedState is returned by Queue.RetryJob when the
// target job is not in the source state requested via
// WithRetryFromState (default: "failed"). BullMQ's reprocessJob-8.lua
// returns -3 in this case.
var ErrJobNotInExpectedState = errors.New("mkq: job is not in the expected source state for retry")
