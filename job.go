package mkq

import "time"

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
}
