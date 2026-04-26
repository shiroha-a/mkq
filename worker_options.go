package mkq

import "time"

// WorkerOption customises a Worker created by Process.
type WorkerOption func(*workerConfig)

type workerConfig struct {
	concurrency      int
	lockDuration     time.Duration
	workerName       string
	idlePollInterval time.Duration
}

// Defaults mirror BullMQ where applicable.
const (
	defaultConcurrency      = 1
	defaultLockDuration     = 30 * time.Second
	defaultIdlePollInterval = 100 * time.Millisecond
)

// WithConcurrency sets the number of in-flight jobs the worker will
// process concurrently. Each slot runs in its own goroutine.
func WithConcurrency(n int) WorkerOption {
	return func(c *workerConfig) { c.concurrency = n }
}

// WithLockDuration sets the lock TTL written to {jobKey}:lock. The
// worker refreshes the lock at half this interval; a missed heartbeat
// past the TTL lets stalled-detection (future PR) reclaim the job.
func WithLockDuration(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.lockDuration = d }
}

// WithWorkerName sets the BullMQ "name" attached to dequeued jobs (the
// HASH `pb` / processedBy field). When empty mkq generates one from
// hostname-pid-uuid at Worker startup.
func WithWorkerName(name string) WorkerOption {
	return func(c *workerConfig) { c.workerName = name }
}

// WithIdlePollInterval controls how long the worker sleeps between
// moveToActive calls when no job is available. A shorter value reduces
// dequeue latency at the cost of Redis load. The marker-based blocking
// dequeue (BullMQ's preferred path) is not yet implemented; this knob
// is the temporary substitute.
func WithIdlePollInterval(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.idlePollInterval = d }
}
