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
// fields (delay, priority, ...) and short keys for the fields listed in
// BullMQ's optsEncodeMap. Most fields here use the long form because
// the lua references them long, but a few (notably "de" for
// Deduplication) use BullMQ's compressed key — the lua reads
// `opts['de']` and round-tripping the long name would diverge from the
// wire format. Add new compressed-key fields with the same explicit
// note if BullMQ extends the map.
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
	// A non-nil value with Count==nil && Age==nil emits an empty
	// object — caller responsibility to use (nil) for that case.
	RemoveOnComplete *RetentionLimit
	RemoveOnFail     *RetentionLimit
	// Deduplication enables BullMQ's `opts.de` deduplication path.
	// nil means "not set" (no dedup); set ID to a non-empty string
	// to opt in.
	Deduplication *Deduplication
}

// Deduplication mirrors the relevant subset of BullMQ's
// `DeduplicationOpts` shape. mkq's first cut exposes only the
// always-supported fields (id, ttl); replace / extend / keepLastIfActive
// can land in follow-ups if mk-go ever needs them.
type Deduplication struct {
	// ID is the dedup correlation key. Required.
	ID string
	// TTLMillis is the dedup window in milliseconds. 0 = no TTL
	// (the key sticks until manually removed or job finalisation
	// clears it via the BullMQ on-finalization include).
	TTLMillis int64
}

// RetentionLimit is BullMQ's `removeOnComplete` / `removeOnFail`
// value shape: optional Count + optional Age. The encoder picks the
// number-shorthand wire form when only Count is set (matching what
// BullMQ TS persists for `removeOnComplete: 5`) and the object form
// `{count?, age?}` when Age is involved.
type RetentionLimit struct {
	// Count: 0 trims immediately, n>0 keeps the most recent n
	// entries. nil omits the count field from the encoded value.
	Count *int
	// AgeSeconds drops entries older than this many seconds. nil
	// omits the age field. BullMQ uses 1-second resolution.
	AgeSeconds *int
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
	if v := encodeRetentionLimit(o.RemoveOnComplete); v != nil {
		m["removeOnComplete"] = v
	}
	if v := encodeRetentionLimit(o.RemoveOnFail); v != nil {
		m["removeOnFail"] = v
	}
	if d := o.Deduplication; d != nil && d.ID != "" {
		de := map[string]any{"id": d.ID}
		if d.TTLMillis > 0 {
			de["ttl"] = d.TTLMillis
		}
		m["de"] = de
	}
	return msgpack.Marshal(m)
}

// encodeRetentionLimit picks the BullMQ wire shape: a bare number
// when only Count is supplied (matching BullMQ TS's `removeOnComplete:
// 5` persistence), or a `{count?, age?}` object when Age is involved.
// Returns nil for fully-unset input so the encoder can omit the key.
func encodeRetentionLimit(r *RetentionLimit) any {
	if r == nil {
		return nil
	}
	if r.AgeSeconds == nil {
		if r.Count == nil {
			return nil
		}
		return *r.Count
	}
	obj := map[string]any{}
	if r.Count != nil {
		obj["count"] = *r.Count
	}
	obj["age"] = *r.AgeSeconds
	return obj
}
