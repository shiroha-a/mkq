package proto

import (
	"github.com/vmihailenco/msgpack/v5"
)

// AddOpts is BullMQ's per-job options bag, encoded as ARGV[3]. Only the
// fields the vendored add* Lua scripts actually read are exposed here;
// future PRs may extend this struct as more features land.
//
// Field naming mirrors what BullMQ stores in the on-Redis HASH, which is
// a JSON-ified version of this same map: long keys for non-compressable
// fields (delay, priority, ...) and short keys ("de", ...) for the
// fields listed in BullMQ's optsEncodeMap. Since the basic add path
// touches none of the compressed fields, every key here uses the long
// form.
type AddOpts struct {
	// Delay is the delay before the job becomes eligible to run, in
	// unix milliseconds offset.
	Delay int64
	// Priority is the BullMQ priority (1..2^21-1, smaller = higher
	// priority). 0 disables prioritization.
	Priority uint32
	// Attempts is the maximum attempts before the job is considered
	// permanently failed.
	Attempts int
	// Backoff configures retry backoff. Nil means no backoff.
	Backoff *Backoff
	// Lifo flips the wait list push direction so newer jobs are
	// processed first.
	Lifo bool
	// RemoveOnComplete / RemoveOnFail mirror BullMQ's per-job
	// retention. nil means "not set" (BullMQ default = keep all).
	// A non-nil value (including 0) is forwarded to Lua: 0 trims
	// immediately, n>0 keeps the most recent n entries.
	RemoveOnComplete *int
	RemoveOnFail     *int
}

// Backoff matches BullMQ's BackoffOptions shape ({type, delay}).
type Backoff struct {
	// Type is "fixed" or "exponential".
	Type string
	// Delay is the base delay in milliseconds.
	Delay int64
}

// EncodeAddOpts returns the msgpack-encoded ARGV[3] payload.
//
// The encoding uses a string-keyed map; missing fields are simply
// omitted so Lua's `opts['x'] or default` idiom yields the right value.
func EncodeAddOpts(o AddOpts) ([]byte, error) {
	m := make(map[string]any, 8)
	if o.Delay != 0 {
		m["delay"] = o.Delay
	}
	if o.Priority != 0 {
		m["priority"] = o.Priority
	}
	if o.Attempts != 0 {
		m["attempts"] = o.Attempts
	}
	if o.Backoff != nil {
		m["backoff"] = map[string]any{
			"type":  o.Backoff.Type,
			"delay": o.Backoff.Delay,
		}
	}
	if o.Lifo {
		m["lifo"] = true
	}
	if o.RemoveOnComplete != nil {
		m["removeOnComplete"] = *o.RemoveOnComplete
	}
	if o.RemoveOnFail != nil {
		m["removeOnFail"] = *o.RemoveOnFail
	}
	return msgpack.Marshal(m)
}
