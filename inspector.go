package mkq

import (
	"context"
	"fmt"

	"github.com/shiroha-a/mkq/internal/lua"
)

// JobBucket identifies a state-key bucket the BullMQ wire format uses
// to partition jobs (waiting / active / etc.). The string values match
// BullMQ verbatim — they're sent into getCounts-1.lua and
// getRanges-1.lua as ARGV, where they're concatenated onto the queue
// prefix to derive the actual Redis key.
//
// The deliberate design choice is to use a string type rather than an
// int enum so foreign callers (admin tooling, log lines) can compare
// by value without reaching into mkq for a stringer.
type JobBucket string

// JobBucket constants align with BullMQ's vendored Lua expectations.
// "wait" / "paused" / "active" are LIST-backed; the rest are ZSET-
// backed. The lua scripts handle the LIST vs ZSET branch internally,
// so callers can mix bucket types in a single Counts / ListJobs call.
const (
	JobBucketWait        JobBucket = "wait"
	JobBucketActive      JobBucket = "active"
	JobBucketDelayed     JobBucket = "delayed"
	JobBucketPrioritized JobBucket = "prioritized"
	JobBucketCompleted   JobBucket = "completed"
	JobBucketFailed      JobBucket = "failed"
	JobBucketPaused      JobBucket = "paused"
)

// allBuckets lists every bucket Counts iterates when the caller
// supplies no explicit set. Matches the order the QueueCounts struct
// exposes them.
var allBuckets = []JobBucket{
	JobBucketWait, JobBucketActive, JobBucketDelayed, JobBucketPrioritized,
	JobBucketCompleted, JobBucketFailed, JobBucketPaused,
}

// QueueCounts is the per-state job count snapshot returned by
// Queue[T].Counts. Fields not requested in the Counts call stay at
// their zero value.
type QueueCounts struct {
	Wait        int64
	Active      int64
	Delayed     int64
	Prioritized int64
	Completed   int64
	Failed      int64
	Paused      int64
}

// Counts returns the number of jobs currently sitting in each of the
// requested buckets. Calling Counts with no buckets returns counts
// for every bucket (matching the BullMQ Queue.getJobCounts() default).
//
// The lua collapses the per-bucket LLEN/ZCARD calls into one round
// trip; pure-Go callers that only need one count are still better
// off via the dedicated helper rather than repeated single-state
// Counts calls.
func (q *Queue[T]) Counts(ctx context.Context, buckets ...JobBucket) (QueueCounts, error) {
	if len(buckets) == 0 {
		buckets = allBuckets
	}

	args := make([]any, 0, len(buckets))
	for _, b := range buckets {
		args = append(args, string(b))
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.GetCounts,
		[]string{q.keys.Base()},
		args...,
	)
	if err != nil {
		return QueueCounts{}, fmt.Errorf("mkq: getCounts: %w", err)
	}
	arr, ok := res.([]any)
	if !ok {
		return QueueCounts{}, fmt.Errorf("mkq: getCounts: unexpected result type %T", res)
	}
	if len(arr) != len(buckets) {
		return QueueCounts{}, fmt.Errorf("mkq: getCounts: result length %d != buckets %d", len(arr), len(buckets))
	}

	var out QueueCounts
	for i, raw := range arr {
		n, _ := toInt64(raw)
		assignBucket(&out, buckets[i], n)
	}
	return out, nil
}

// assignBucket dispatches a per-bucket count into the corresponding
// QueueCounts field. Unknown buckets are dropped silently — they
// never reach this path because allBuckets / caller-supplied values
// are validated by the lua's branch matching, but the default case
// keeps the function safe against future bucket additions.
func assignBucket(c *QueueCounts, b JobBucket, n int64) {
	switch b {
	case JobBucketWait:
		c.Wait = n
	case JobBucketActive:
		c.Active = n
	case JobBucketDelayed:
		c.Delayed = n
	case JobBucketPrioritized:
		c.Prioritized = n
	case JobBucketCompleted:
		c.Completed = n
	case JobBucketFailed:
		c.Failed = n
	case JobBucketPaused:
		c.Paused = n
	}
}
