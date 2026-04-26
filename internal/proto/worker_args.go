package proto

import (
	"github.com/vmihailenco/msgpack/v5"
)

// MoveToActiveOpts is the BullMQ "opts" map consumed by
// moveToActive-11.lua via cmsgpack.unpack of ARGV[3].
//
// The Lua reads opts['token'], opts['lockDuration'], opts['name'], and
// optionally opts['limiter']{'max','duration'}; only token+lockDuration
// are required. Limiter is omitted in the basic worker path and stays
// here as a future hook.
type MoveToActiveOpts struct {
	// Token is the per-job lock token the worker writes to
	// {jobKey}:lock. The worker is then expected to extend it via
	// extendLock until the handler finishes.
	Token string
	// LockDuration is the lock TTL in milliseconds.
	LockDuration int64
	// Name is the worker name; Lua mirrors it into the HASH `pb`
	// (processedBy) field for stalled-recovery attribution.
	Name string
}

// EncodeMoveToActiveOpts returns the msgpack-encoded ARGV[3] payload.
func EncodeMoveToActiveOpts(o MoveToActiveOpts) ([]byte, error) {
	m := map[string]any{
		"token":        o.Token,
		"lockDuration": o.LockDuration,
	}
	if o.Name != "" {
		m["name"] = o.Name
	}
	return msgpack.Marshal(m)
}
