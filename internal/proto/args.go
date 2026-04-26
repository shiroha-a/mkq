// Package proto encodes the wire payloads BullMQ Lua scripts expect on
// EVALSHA.
//
// BullMQ packs its ARGV in three slots:
//
//	ARGV[1] — msgpack-encoded array of positional args (key prefix,
//	          custom id, job name, timestamp, plus parent/repeat/dedupe
//	          fields that are nil in the basic add path).
//	ARGV[2] — JSON-encoded job payload.
//	ARGV[3] — msgpack-encoded options map (delay, priority, attempts,
//	          backoff, lifo, ...).
//
// proto exposes pure functions that build ARGV[1] and ARGV[3]; ARGV[2]
// is just json.Marshal at the call site.
package proto

import (
	"github.com/vmihailenco/msgpack/v5"
)

// AddArgs are the positional arguments that the addStandardJob /
// addDelayedJob / addPrioritizedJob Lua scripts read from ARGV[1].
//
// Fields not relevant to the basic add path (Parent, Dependencies,
// RepeatJobKey, DeduplicationKey) are encoded as msgpack nil and the
// vendored Lua handles them as such.
type AddArgs struct {
	// Prefix is BullMQ's "key prefix" (e.g. "bull:deliver:"), used by
	// Lua to derive {prefix}{jobId}.
	Prefix string
	// CustomID is the user-supplied job id, or "" to let Redis INCR
	// generate one. Lua checks `args[2] == ""` to choose.
	CustomID string
	// Name is the job name (what BullMQ calls Job.name).
	Name string
	// Timestamp is the creation time in unix milliseconds.
	Timestamp int64
	// ParentKey, ParentDependencies, Parent, RepeatJobKey,
	// DeduplicationKey are reserved for parent/dedupe/repeat features
	// that are not yet exposed in the public API.
	ParentKey          string
	ParentDependencies string
	Parent             *ParentRef
	RepeatJobKey       string
	DeduplicationKey   string
}

// ParentRef is the BullMQ {id, queueKey} parent record. Encoded as a
// msgpack array of two strings to match BullMQ's lua handling.
type ParentRef struct {
	ID       string
	QueueKey string
}

// EncodeAddArgs returns the msgpack-encoded ARGV[1] payload.
func EncodeAddArgs(a AddArgs) ([]byte, error) {
	arr := []any{
		a.Prefix,
		a.CustomID,
		a.Name,
		a.Timestamp,
		nilOrString(a.ParentKey),
		nilOrString(a.ParentDependencies),
		parentToAny(a.Parent),
		nilOrString(a.RepeatJobKey),
		nilOrString(a.DeduplicationKey),
	}
	return msgpack.Marshal(arr)
}

func nilOrString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func parentToAny(p *ParentRef) any {
	if p == nil {
		return nil
	}
	// BullMQ encodes parent as a JSON object {id, queueKey}; in lua it
	// becomes a table. msgpack maps preserve string keys verbatim.
	return map[string]string{
		"id":       p.ID,
		"queueKey": p.QueueKey,
	}
}
