package mkq

import "time"

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
