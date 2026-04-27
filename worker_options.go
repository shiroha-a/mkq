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
	maxDataPoints    int // 0 = disabled (BullMQ-spec metrics off)
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

// WithIdlePollInterval bounds how long the worker may block waiting
// for a marker-key push from the BullMQ Lua side. New jobs wake the
// dispatcher within ms via BZPopMin on `{prefix}:{queue}:marker`;
// this knob is the timeout ceiling that fires when nothing has
// landed (e.g. so a Redis outage doesn't strand a worker indefinitely).
//
// Floor: Redis BZPOPMIN takes a second-granularity timeout (Redis 6+
// supports fractional seconds, but go-redis floors anything below 1s
// with a warning), so the effective floor is 1s regardless of what
// you pass here. Sub-second values are harmless — they still drive
// the dispatch-latency improvement (marker push wakes within ms) but
// the BZPopMin ceiling stays at 1s. ctx cancellation is unaffected:
// Worker.Stop returns immediately because go-redis honours ctx on
// blocking commands.
//
// A blocked BZPopMin holds one Redis connection per worker slot, so
// WithConcurrency(N) callers should ensure their go-redis
// UniversalOptions.PoolSize is >= N + a few; the default
// 10*GOMAXPROCS is plenty for concurrencies up to a few dozen.
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
//
// Precedence note: BullMQ also supports a queue-level rate limit
// stored in the meta HASH (`max` / `duration` keys). The vendored
// moveToActive Lua reads that first and falls back to opts.limiter,
// so a queue-level setting written by another client (e.g. a
// BullMQ TS admin) silently overrides WithRateLimit. mkq does not
// yet expose a public API to write the meta keys.
func WithRateLimit(max int, duration time.Duration) WorkerOption {
	return func(c *workerConfig) {
		if max <= 0 || duration <= 0 {
			c.limiter = nil
			return
		}
		c.limiter = &workerLimiter{max: max, duration: duration}
	}
}

// WithJobMetrics enables BullMQ-spec per-minute job-count metrics
// (the `bull:<queue>:metrics:completed` / `:failed` HASH + `:data`
// LIST). maxDataPoints caps the number of one-minute buckets kept
// in the data LIST (LTRIM after each finalise); <=0 disables the
// feature, matching BullMQ TS's `metrics: { maxDataPoints }` option
// on `WorkerOptions`.
//
// Once enabled, every moveToFinished call by this worker writes a
// bucket atomically alongside the state transition. Read the data
// back via Queue.GetMetrics; bull-board / Misskey admin and other
// BullMQ-aware dashboards see it directly because the keys and
// layout are wire-compatible with BullMQ v5+.
//
// The option name `JobMetrics` disambiguates from the unrelated
// `mkq.Metrics` observability interface (Logger / Metrics / Tracer)
// configured on Config.
func WithJobMetrics(maxDataPoints int) WorkerOption {
	return func(c *workerConfig) {
		if maxDataPoints < 0 {
			maxDataPoints = 0
		}
		c.maxDataPoints = maxDataPoints
	}
}
