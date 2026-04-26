package mkq

import (
	"encoding/json"
	"time"
)

// Job represents a job that has been enqueued via Queue.Add. The struct
// is a snapshot of the wire-level state at the moment of enqueue;
// progress / returnvalue / state transitions are not reflected here
// (that surface arrives with the worker API in a follow-up PR).
type Job[T any] struct {
	// ID is the BullMQ job id. Numeric for auto-allocated jobs;
	// arbitrary string when WithJobID is used.
	ID string
	// Name is the BullMQ job name. mkq sets this to the queue name
	// today; future work may expose per-Add naming.
	Name string
	// Data is the user payload, as it was passed to Add.
	Data T
	// Timestamp is the creation time at millisecond precision; it
	// matches the value Lua wrote to the HASH `timestamp` field.
	Timestamp time.Time

	// AttemptsMade is the BullMQ `atm` counter — how many times the
	// job has been moved to retry / failed before this attempt.
	// Populated from the moveToActive HGETALL snapshot, so handler
	// callers see the value as observed at dequeue time.
	AttemptsMade int

	// AttemptsStarted is BullMQ `ats` — total number of moveToActive
	// dequeues for this job (incremented even when stalled-recovery
	// re-acquires it).
	AttemptsStarted int

	// StalledCounter is BullMQ `stc` — how many times stalled
	// detection reclaimed this job. Once it exceeds
	// WithMaxStalledCount, BullMQ writes the canonical default-failed
	// reason into `defa`.
	StalledCounter int

	// ProcessedBy is BullMQ `pb` — the worker name that most recently
	// dequeued this job (set by prepareJobForProcessing.lua).
	ProcessedBy string

	// queue is set when the Job is constructed via a Queue path
	// (Add / Get / handler dispatch), enabling the mutation methods
	// (UpdateProgress / UpdateData / Log) without forcing callers to
	// thread the queue separately. Stays nil for synthetic Jobs
	// constructed by tests; methods then return ErrJobDetached.
	queue *Queue[T]
}

// JobState is the post-finalisation read-only snapshot of a job's
// HASH. Returned by Queue.Get for admin-style inspection. Fields
// remain at their zero value when the job hasn't reached the
// corresponding state (e.g. ProcessedOn is zero for jobs still in
// wait).
type JobState struct {
	// ProcessedOn is the moment moveToActive last picked the job up.
	// Zero for jobs that have never been dequeued.
	ProcessedOn time.Time
	// FinishedOn is the moment moveToFinished sealed the job. Zero
	// when the job is still in flight or queued.
	FinishedOn time.Time
	// ReturnValue is the BullMQ `returnvalue` HASH field (raw JSON,
	// the user's handler return as JSON.stringify-d). Empty when the
	// job hasn't completed.
	ReturnValue json.RawMessage
	// FailedReason is the BullMQ `failedReason` HASH field, stored as
	// a plain string (not JSON) per BullMQ TS convention.
	FailedReason string
	// Stacktrace is the BullMQ `stacktrace` HASH field, decoded from
	// its JSON-array-of-strings shape.
	Stacktrace []string
	// Progress is the BullMQ `progress` HASH field (raw JSON, can be
	// any value — number, string, object).
	Progress json.RawMessage
}
