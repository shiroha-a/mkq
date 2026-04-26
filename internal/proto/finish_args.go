package proto

import (
	"errors"

	"github.com/vmihailenco/msgpack/v5"
)

// MoveToFinishedOpts is the BullMQ "opts" map consumed by
// moveToFinished-14.lua via cmsgpack.unpack of ARGV[8].
//
// The Lua reads opts['token'] / opts['lockDuration'] for ownership
// validation, opts['attempts'] for retry decisions, opts['keepJobs']
// for retention trimming, and opts['name'] for attribution. Worker
// fields the basic happy-path doesn't expose (fpof, cpof, idof, rdof,
// maxMetricsSize) stay omitted.
type MoveToFinishedOpts struct {
	Token        string
	LockDuration int64
	// Attempts mirrors the user-facing WithAttempts. Lua uses it to
	// decide whether a failure should be retried; mkq's first worker
	// PR does not implement retry so this is informational only.
	Attempts int
	// KeepJobs configures retention for the completed/failed ZSET.
	// Nil means BullMQ's default retention (keep all).
	KeepJobs *KeepJobs
	Name     string
}

// KeepJobs mirrors BullMQ's keepJobs option. Either Count or Age can
// be set; both forms supported by storeAndEnqueueJob etc.
type KeepJobs struct {
	// Count keeps at most N completed/failed jobs.
	Count int
	// Age (seconds) drops jobs older than this from the set.
	Age int
}

// EncodeMoveToFinishedOpts returns the msgpack-encoded ARGV[8] payload.
//
// keepJobs is always emitted (as an empty map when unset) because the
// vendored moveToFinished Lua dereferences `opts['keepJobs']['count']`
// without a nil guard; sending nil would surface as a runtime "attempt
// to index a nil value" error from Redis.
func EncodeMoveToFinishedOpts(o MoveToFinishedOpts) ([]byte, error) {
	keep := map[string]any{}
	if o.KeepJobs != nil {
		if o.KeepJobs.Count > 0 {
			keep["count"] = o.KeepJobs.Count
		}
		if o.KeepJobs.Age > 0 {
			keep["age"] = o.KeepJobs.Age
		}
	}
	m := map[string]any{
		"token":        o.Token,
		"lockDuration": o.LockDuration,
		"attempts":     o.Attempts,
		"keepJobs":     keep,
	}
	if o.Name != "" {
		m["name"] = o.Name
	}
	return msgpack.Marshal(m)
}

// EncodeJobFields packs ARGV[9] of moveToFinished — extra HASH fields
// to write atomically along with the state transition. Each call site
// passes whichever of `processedOn` / `finishedOn` / `returnvalue` /
// `failedReason` it owns as alternating key/value pairs.
//
// The wire format is a msgpack-encoded flat array `[k1, v1, k2, v2,
// ...]` because the vendored includes/updateJobFields.lua does
// `cmsgpack.unpack(msgpackedFields)` and forwards the result to HMSET
// via `unpack(...)`.
func EncodeJobFields(pairs ...any) ([]byte, error) {
	if len(pairs)%2 != 0 {
		return nil, errOddJobFields
	}
	if len(pairs) == 0 {
		return nil, nil
	}
	return msgpack.Marshal(pairs)
}

var errOddJobFields = errors.New("proto: EncodeJobFields requires an even number of arguments")
