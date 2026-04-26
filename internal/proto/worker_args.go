package proto

import (
	"github.com/vmihailenco/msgpack/v5"
)

// MoveToActiveOpts is the BullMQ "opts" map consumed by
// moveToActive-11.lua via cmsgpack.unpack of ARGV[3].
//
// The Lua reads opts['token'], opts['lockDuration'], opts['name'], and
// optionally opts['limiter']{'max','duration'}; only token+lockDuration
// are required.
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
	// Limiter, when non-nil, activates per-Worker rate limiting.
	// The vendored Lua's getRateLimitTTL reads Max+DurationMs and
	// returns the remaining cooldown in expireTime when the limit
	// is hit, so the worker can back off precisely.
	Limiter *MoveToActiveLimiter
}

// MoveToActiveLimiter mirrors BullMQ's worker.limiter option.
// DurationMs is the rolling-window length in milliseconds.
type MoveToActiveLimiter struct {
	Max        int
	DurationMs int64
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
	if o.Limiter != nil {
		m["limiter"] = map[string]any{
			"max":      o.Limiter.Max,
			"duration": o.Limiter.DurationMs,
		}
	}
	return msgpack.Marshal(m)
}
