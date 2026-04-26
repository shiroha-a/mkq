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
