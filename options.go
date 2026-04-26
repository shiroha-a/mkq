package mkq

import "time"

// AddOption customises a single Queue.Add call.
type AddOption func(*addConfig)

// QueueOption customises a Queue at Define time.
type QueueOption func(*queueConfig)

type queueConfig struct {
	// reserved for follow-up PRs (concurrency, rate limit, retention).
}

type addConfig struct {
	jobID            string
	delay            time.Duration
	attempts         int
	priority         uint32
	lifo             bool
	backoff          *BackoffStrategy
	keepCompleted    *int
	keepFailed       *int
	keepCompletedAge *time.Duration
	keepFailedAge    *time.Duration
}

// WithJobID forces the job id instead of letting Redis INCR allocate
// one. BullMQ uses this to deduplicate enqueue at the application
// layer ("if a job with this id already exists, do nothing").
func WithJobID(id string) AddOption {
	return func(c *addConfig) { c.jobID = id }
}

// WithDelay schedules the job to first become eligible after d. The
// job lands on the delayed ZSET; a worker (or scheduler tick)
// promotes it once the timestamp is reached.
func WithDelay(d time.Duration) AddOption {
	return func(c *addConfig) { c.delay = d }
}

// WithAttempts sets the maximum number of attempts before the job is
// considered permanently failed.
func WithAttempts(n int) AddOption {
	return func(c *addConfig) { c.attempts = n }
}

// WithPriority sets the BullMQ priority. Valid range is 1..2^21-1;
// smaller numbers run first. 0 disables prioritization (the job lands
// on the wait LIST instead of the prioritized ZSET).
func WithPriority(p uint32) AddOption {
	return func(c *addConfig) { c.priority = p }
}

// WithLifo flips the wait list push direction so the job is processed
// LIFO instead of FIFO. Has no effect on priority or delayed jobs.
func WithLifo(lifo bool) AddOption {
	return func(c *addConfig) { c.lifo = lifo }
}

// WithBackoff sets the retry backoff strategy.
func WithBackoff(b BackoffStrategy) AddOption {
	return func(c *addConfig) { c.backoff = &b }
}

// WithKeepCompleted bounds the size of the completed ZSET. n==0 makes
// each completed job removed immediately (BullMQ removeOnComplete=true);
// n>0 keeps the most recent n entries via the vendored
// removeJobsByMaxCount Lua. Not calling WithKeepCompleted preserves
// BullMQ's default (keep all completed jobs).
//
// n must be >= 0. Negative values are forwarded as-is to the
// vendored Lua, where BullMQ's `maxCount > 0` guard skips trimming —
// the practical effect is identical to "keep all", but the value is
// not normalised here so callers should treat negatives as
// programmer error.
func WithKeepCompleted(n int) AddOption {
	return func(c *addConfig) { c.keepCompleted = &n }
}

// WithKeepFailed bounds the size of the failed ZSET, mirroring
// WithKeepCompleted's semantics (and its n>=0 expectation) for the
// failure path.
func WithKeepFailed(n int) AddOption {
	return func(c *addConfig) { c.keepFailed = &n }
}

// WithKeepCompletedAge drops completed jobs older than age relative
// to each subsequent finalisation tick. Mirrors BullMQ's
// `removeOnComplete: { age: <seconds> }`. Resolution is one second;
// sub-second values round to zero (no trim).
//
// Combine with WithKeepCompleted to cap by both count and age in
// the same call (BullMQ's `{count, age}` shape).
func WithKeepCompletedAge(age time.Duration) AddOption {
	return func(c *addConfig) { c.keepCompletedAge = &age }
}

// WithKeepFailedAge is the failed-side analogue of
// WithKeepCompletedAge.
func WithKeepFailedAge(age time.Duration) AddOption {
	return func(c *addConfig) { c.keepFailedAge = &age }
}
