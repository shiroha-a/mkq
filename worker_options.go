package mkq

import "time"

// WorkerOption customises a Worker created by Process.
type WorkerOption func(*workerConfig)

type workerConfig struct {
	concurrency      int
	lockDuration     time.Duration
	workerName       string
	idlePollInterval time.Duration
	stalledInterval  time.Duration
	maxStalledCount  int
	limiter          *workerLimiter
}

type workerLimiter struct {
	max      int
	duration time.Duration
}

// Defaults mirror BullMQ where applicable.
const (
	defaultConcurrency      = 1
	defaultLockDuration     = 30 * time.Second
	defaultIdlePollInterval = 100 * time.Millisecond
	defaultStalledInterval  = 30 * time.Second
	defaultMaxStalledCount  = 1
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

// WithStalledInterval controls how often each Worker scans for jobs
// whose lock has expired (a worker that crashed or lost its Redis
// connection). The Lua's stalled-check key TTL is set to this value
// so concurrent workers serialise the scan.
//
// BullMQ's default is 30s and works well for most production setups;
// shorter intervals shorten the recovery window at the cost of more
// Redis traffic.
func WithStalledInterval(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.stalledInterval = d }
}

// WithMaxStalledCount sets how many times a single job may be
// reclaimed by stalled detection before BullMQ marks it as failed
// outright (HASH `defa` = "job stalled more than allowable limit").
// Default 1 matches BullMQ.
func WithMaxStalledCount(n int) WorkerOption {
	return func(c *workerConfig) { c.maxStalledCount = n }
}

// WithRateLimit caps Worker dequeue throughput at max jobs per the
// given rolling-window duration. Mirrors BullMQ's worker.limiter
// option: when the cap is reached, the vendored Lua returns the
// remaining cooldown via expireTime and the dispatch loop sleeps
// exactly that long before its next moveToActive attempt.
//
// max <= 0 or duration <= 0 disables rate limiting (the default).
func WithRateLimit(max int, duration time.Duration) WorkerOption {
	return func(c *workerConfig) {
		if max <= 0 || duration <= 0 {
			c.limiter = nil
			return
		}
		c.limiter = &workerLimiter{max: max, duration: duration}
	}
}
