package mkq

import "errors"

// ErrPriorityWithDelay is returned by Queue.Add when both WithPriority
// and WithDelay are supplied. BullMQ stores the priority in the HASH
// but the dispatch is delayed-only, so the priority would not affect
// dequeue ordering — surfacing the conflict avoids a silent footgun.
//
// A future PR may add a combined dispatch (delayed + prioritized) that
// removes this restriction.
var ErrPriorityWithDelay = errors.New("mkq: WithPriority cannot be combined with WithDelay")

// ErrUnrecoverable, when wrapped into a handler's returned error,
// instructs the worker to skip the retry path even if WithAttempts
// would otherwise allow another attempt. The job transitions straight
// to failed.
//
// This mirrors BullMQ's UnrecoverableError class for foreign-language
// workers: bull-board reports the same final-failed shape regardless
// of whether the rejection came from a JS Worker or mkq.
//
//	return fmt.Errorf("invalid input: %w", mkq.ErrUnrecoverable)
var ErrUnrecoverable = errors.New("mkq: unrecoverable error (skip retry)")
