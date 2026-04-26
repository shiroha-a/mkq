package mkq

import (
	"math"
	"time"
)

// BackoffStrategy describes a BullMQ-compatible backoff. The on-Redis
// JSON shape is {"type": "...", "delay": <ms>}; mkq mirrors it
// verbatim.
type BackoffStrategy struct {
	// Type is "fixed" or "exponential" — the values BullMQ recognises
	// out of the box. Custom strategies (BullMQ's "custom" type) are
	// out of scope for the initial release.
	Type string
	// Delay is the base delay; Type controls how it scales between
	// retries.
	Delay time.Duration
}

// FixedBackoff retries on the same delay every attempt.
func FixedBackoff(d time.Duration) BackoffStrategy {
	return BackoffStrategy{Type: "fixed", Delay: d}
}

// ExponentialBackoff doubles the delay each attempt. BullMQ's built-in
// exponential strategy does not accept a cap; callers that want a
// ceiling will need a custom strategy in a follow-up release.
func ExponentialBackoff(d time.Duration) BackoffStrategy {
	return BackoffStrategy{Type: "exponential", Delay: d}
}

// computeBackoffDelay mirrors BullMQ's Backoffs.calculate worker-side
// computation: the chosen delay is passed to Lua as a plain integer
// so the wire-level retry script (retryJob / moveToDelayed) does not
// know about strategy types.
//
// attemptsMade is the post-bump count (i.e. "this is attempt N");
// BullMQ's exponential formula is `delay * 2^(attemptsMade-1)`.
//
// Returns 0 for an unset / nil strategy. Overflow is clamped to
// math.MaxInt64 nanoseconds so a misconfigured exponential never
// wraps negative.
func computeBackoffDelay(b *BackoffStrategy, attemptsMade int) time.Duration {
	if b == nil || attemptsMade < 1 {
		return 0
	}
	switch b.Type {
	case "fixed":
		return b.Delay
	case "exponential":
		shift := uint(attemptsMade - 1)
		// 上限を 62 bit に抑える: 1<<63 は signed int64 で overflow。
		if shift >= 63 {
			return time.Duration(math.MaxInt64)
		}
		mult := time.Duration(1) << shift
		// b.Delay * mult が int64 を overflow しないかを乗算前にチェック。
		if mult != 0 && b.Delay > time.Duration(math.MaxInt64)/mult {
			return time.Duration(math.MaxInt64)
		}
		return b.Delay * mult
	default:
		return 0
	}
}
