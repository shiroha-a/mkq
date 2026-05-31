package mkq

import (
	"math"
	"time"
)

// BackoffStrategy describes a BullMQ-compatible backoff. The on-Redis
// JSON shape is {"type": "...", "delay": <ms>, "jitter": <0..1>};
// mkq mirrors it verbatim.
type BackoffStrategy struct {
	// Type is "fixed", "exponential" (the values BullMQ recognises out
	// of the box), or "custom" (BullMQ's settings.backoffStrategy
	// path). For "custom", register the computation on the Worker via
	// WithBackoffStrategy.
	Type string
	// Delay is the base delay; Type controls how it scales between
	// retries. Ignored for "custom".
	Delay time.Duration
	// Jitter is the BullMQ jitter fraction (0..1) applied to fixed /
	// exponential delays to spread retries and ease thundering herds.
	// 0 disables jitter. Ignored for "custom" (the registered strategy
	// owns any jitter it wants).
	Jitter float64
}

// CustomBackoffFunc computes the retry delay for a job whose backoff
// Type is not a BullMQ built-in (e.g. "custom"). attemptsMade is the
// post-bump attempt count (i.e. "this is attempt N"), matching the
// argument BullMQ passes to settings.backoffStrategy.
//
// This is where a caller reproduces an arbitrary formula plus cap plus
// jitter Go-side. mkq never persists a cap to Redis (BullMQ has no cap
// field, so a foreign worker would ignore it); capping belongs here.
type CustomBackoffFunc func(attemptsMade int) time.Duration

// FixedBackoff retries on the same delay every attempt.
func FixedBackoff(d time.Duration) BackoffStrategy {
	return BackoffStrategy{Type: "fixed", Delay: d}
}

// FixedBackoffWithJitter is FixedBackoff with a BullMQ jitter fraction
// (0..1). The effective delay lands in [d*(1-jitter), d).
func FixedBackoffWithJitter(d time.Duration, jitter float64) BackoffStrategy {
	return BackoffStrategy{Type: "fixed", Delay: d, Jitter: jitter}
}

// ExponentialBackoff doubles the delay each attempt (delay * 2^(n-1)).
// BullMQ's built-in exponential strategy does not accept a cap; callers
// that want a ceiling should use a custom strategy (see CustomBackoff /
// WithBackoffStrategy).
func ExponentialBackoff(d time.Duration) BackoffStrategy {
	return BackoffStrategy{Type: "exponential", Delay: d}
}

// ExponentialBackoffWithJitter is ExponentialBackoff with a BullMQ
// jitter fraction (0..1). For attempt n the un-jittered delay is
// delay*2^(n-1); the jittered result lands in [delay*2^(n-1)*(1-jitter),
// delay*2^(n-1)).
func ExponentialBackoffWithJitter(d time.Duration, jitter float64) BackoffStrategy {
	return BackoffStrategy{Type: "exponential", Delay: d, Jitter: jitter}
}

// CustomBackoff marks a job as using the Worker-registered backoff
// strategy (BullMQ's settings.backoffStrategy path). The on-Redis opts
// store backoff as {"type": "custom"}; the actual delay is computed by
// the CustomBackoffFunc registered via WithBackoffStrategy on the Worker
// that processes the job.
//
// This is the most flexible option: arbitrary formula, cap, and jitter
// all live in the registered Go function, so mk-go can reproduce
// Misskey's httpRelatedBackoff ((2^n-1)*base, capped at 8h, +0..20%
// jitter) verbatim.
func CustomBackoff() BackoffStrategy {
	return BackoffStrategy{Type: "custom"}
}

// computeBackoffDelay mirrors BullMQ's Backoffs.calculate worker-side
// computation for the built-in fixed / exponential strategies. It
// returns the un-jittered base delay; jitter (and custom strategies)
// are layered on by Worker.computeRetryDelay. The chosen delay is
// passed to Lua as a plain integer so the wire-level retry script
// (retryJob / moveToDelayed) does not know about strategy types.
//
// attemptsMade is the post-bump count (i.e. "this is attempt N");
// BullMQ's exponential formula is `delay * 2^(attemptsMade-1)`.
//
// Returns 0 for an unset / nil strategy or any non-built-in type.
// Overflow is clamped to math.MaxInt64 nanoseconds so a misconfigured
// exponential never wraps negative.
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

// applyJitter reproduces BullMQ's built-in jitter formula:
//
//	floor(random() * base * jitter + base * (1 - jitter))
//
// base is the un-jittered delay (delay for fixed, delay*2^(n-1) for
// exponential), r is a random value in [0, 1), and jitter is the
// fraction in (0, 1]. The result lands in [base*(1-jitter), base).
// BullMQ's Math.floor is reproduced by the float64 -> time.Duration
// truncation.
//
// jitter values outside (0, 1] are clamped so a misconfigured fraction
// never produces a negative or above-base delay.
func applyJitter(base time.Duration, jitter, r float64) time.Duration {
	if jitter <= 0 {
		return base
	}
	if jitter > 1 {
		jitter = 1
	}
	minDelay := float64(base) * (1 - jitter)
	return time.Duration(r*float64(base)*jitter + minDelay)
}
