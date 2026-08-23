package mkq

import (
	"encoding/json"
	"time"
)

// Job represents a job that has been enqueued via Queue.Add or
// reconstructed by the worker dispatch path. The exported fields are
// a wire-level snapshot taken at construction time and never refresh
// in place; for the post-finalisation view of the same job (progress,
// return value, finished/processed timestamps), call Queue.Get and
// inspect the JobState companion.
//
// Mutations performed via the methods on this type (UpdateProgress,
// UpdateData, Log) write through to Redis directly via the BullMQ
// vendored Lua scripts; they do not update the local Job struct.
type Job[T any] struct {
	// ID is the BullMQ job id. Numeric for auto-allocated jobs;
	// arbitrary string when WithJobID is used.
	ID string
	// Name is the BullMQ job name. Defaults to the queue name; can
	// be overridden per Add via WithJobName, which is the recommended
	// way to fan one queue out across multiple task types.
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
	// Opts is the BullMQ `opts` HASH field verbatim (raw JSON). It is
	// what BullMQ's TS client exposes as `Job.opts`: attempts, backoff,
	// removeOnComplete / removeOnFail, priority and so on.
	//
	// **加工せずそのまま返す。** admin UI はこれを丸ごと表示する用途で読む
	// ので、mkq が知っている key だけを組み立て直すと、知らない key が
	// 黙って消える。Add した側が渡した内容と 1 対 1 で見えることに意味がある。
	Opts json.RawMessage
	// Delay is the BullMQ `delay` HASH field in milliseconds. Zero for
	// jobs added without a delay.
	Delay int64
	// AttemptsAt holds the start time of every failed attempt, oldest
	// first, in unix milliseconds.
	//
	// **BullMQ には対応物が無い。** BullMQ TS も試行ごとの時刻を残さないので、
	// 再試行を時系列に並べたい admin UI は置く時刻を持てない (upstream Misskey の
	// job 詳細が試行を `at ?` と表示しているのはこれが理由)。mkq の拡張として
	// 記録する。未知 field は BullMQ / bull-board が無視するので wire 互換は保たれる。
	//
	// **この拡張が入る前に失敗した job には無い。** 遡って埋めることはできない。
	AttemptsAt []int64
}
