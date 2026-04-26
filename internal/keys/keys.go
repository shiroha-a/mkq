// Package keys builds BullMQ-compatible Redis keys.
//
// BullMQ namespaces every queue under "{prefix}:{queue}:..." with ":" as a
// separator. The exact set of keys mkq cares about is documented in
// mkq-design.md §5.1; the helpers below mirror BullMQ verbatim so vendored
// Lua scripts and foreign workers see identical layout.
package keys

import "strconv"

// DefaultPrefix is BullMQ's default keyPrefix when none is configured.
const DefaultPrefix = "bull"

// Builder produces the per-queue key set. Build it once per Queue and
// reuse: every method is a deterministic string concatenation.
type Builder struct {
	// Prefix is the BullMQ "keyPrefix" (e.g. "bull" or "host:bull").
	Prefix string
	// Queue is the queue name (e.g. "deliver").
	Queue string
}

// New constructs a Builder, defaulting Prefix to DefaultPrefix when empty.
func New(prefix, queue string) Builder {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return Builder{Prefix: prefix, Queue: queue}
}

// Base returns the queue-level prefix that all keys share, including the
// trailing ":". BullMQ Lua scripts pass this as args[1] (key prefix).
func (b Builder) Base() string {
	return b.Prefix + ":" + b.Queue + ":"
}

// Wait, Active, Delayed, Prioritized, Completed, Failed, Paused — top
// level state keys.
func (b Builder) Wait() string        { return b.Base() + "wait" }
func (b Builder) Active() string      { return b.Base() + "active" }
func (b Builder) Delayed() string     { return b.Base() + "delayed" }
func (b Builder) Prioritized() string { return b.Base() + "prioritized" }
func (b Builder) Completed() string   { return b.Base() + "completed" }
func (b Builder) Failed() string      { return b.Base() + "failed" }
func (b Builder) Paused() string      { return b.Base() + "paused" }
func (b Builder) Stalled() string     { return b.Base() + "stalled" }

// Meta is the queue-global HASH (paused flag, limiter config, etc.).
func (b Builder) Meta() string { return b.Base() + "meta" }

// ID is the INCR counter for auto-generated job ids.
func (b Builder) ID() string { return b.Base() + "id" }

// Marker is BullMQ v5+'s blocking-worker wakeup ZSET.
func (b Builder) Marker() string { return b.Base() + "marker" }

// PriorityCounter (BullMQ "pc") breaks ties between equal-priority jobs
// to preserve FIFO within a priority bucket.
func (b Builder) PriorityCounter() string { return b.Base() + "pc" }

// Events is the Redis Stream that BullMQ QueueEvents subscribes to.
func (b Builder) Events() string { return b.Base() + "events" }

// Limiter is the rate-limiter token bucket HASH.
func (b Builder) Limiter() string { return b.Base() + "limiter" }

// Repeat is the ZSET of repeat job templates.
func (b Builder) Repeat() string { return b.Base() + "repeat" }

// Job returns the per-job HASH key.
func (b Builder) Job(jobID string) string { return b.Base() + jobID }

// JobLogs returns the per-job log LIST key.
func (b Builder) JobLogs(jobID string) string { return b.Job(jobID) + ":logs" }

// JobLock returns the per-job lock STRING key.
func (b Builder) JobLock(jobID string) string { return b.Job(jobID) + ":lock" }

// JobUint is a convenience for callers that have a numeric job id.
func (b Builder) JobUint(jobID uint64) string {
	return b.Base() + strconv.FormatUint(jobID, 10)
}
