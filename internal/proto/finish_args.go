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
	// Attempts mirrors the user-facing WithAttempts. The vendored Lua
	// gates the `retries-exhausted` event on
	// `attemptsMade >= attempts`, so callers that go straight to
	// failed (e.g. ErrUnrecoverable) must still pass the job's
	// configured attempts to avoid emitting a spurious exhaustion
	// event into the wire-format events stream.
	Attempts int
	// KeepJobs configures retention for the completed/failed ZSET.
	// Nil means BullMQ's default retention (keep all).
	KeepJobs *KeepJobs
	Name     string
	// Limiter mirrors the moveToActive limiter shape; required when
	// fetchNext=1 so the Lua's fetchNextJob branch can read
	// opts['limiter']['max'] / ['duration'] for the rate-limit
	// gating that protects throughput. Nil = no limiter.
	Limiter *MoveToActiveLimiter
}

// KeepJobs mirrors BullMQ's keepJobs option. Either field can be set;
// both forms are supported by storeAndEnqueueJob etc. Pointers
// distinguish "unset" from "explicitly zero" — the Lua treats
// keepJobs.count==0 as "remove immediately", so an age-only caller
// must NOT default-initialise count.
type KeepJobs struct {
	// Count: nil = unbounded; *0 = remove immediately; *n>0 = keep
	// the most recent n entries via removeJobsByMaxCount.
	Count *int
	// Age (seconds) drops jobs older than this from the set on every
	// finalisation tick. 0 / nil disables age trimming.
	Age int
}

// EncodeMoveToFinishedOpts returns the msgpack-encoded ARGV[8] payload.
//
// Two fields are always emitted regardless of caller input because the
// vendored Lua dereferences them without nil guards:
//
//   - keepJobs: opts['keepJobs']['count'] is read directly. nil would
//     produce "attempt to index a nil value".
//   - maxMetricsSize: collectMetrics() does
//     `tonumber(maxDataPoints)` which crashes with "bad argument" if
//     maxDataPoints is nil but the outer guard `if maxMetricsSize ~=
//     ""` permitted entry. BullMQ TypeScript sends "" when metrics
//     are disabled; mkq mirrors that.
func EncodeMoveToFinishedOpts(o MoveToFinishedOpts) ([]byte, error) {
	// Hot path: invoked once per finalised job. Uses the package
	// encoder pool (pool.go) so msgpack.Encoder + bytes.Buffer are
	// reused — only the returned byte slice allocates per call.
	return encodeMsgpack(func(enc *msgpack.Encoder) error {
		keep := map[string]any{}
		if o.KeepJobs != nil {
			// Count==nil omits the count key, leaving Lua's
			// `opts['keepJobs']['count']` as nil (treated as unbounded).
			// Count==*0 explicitly emits 0 (BullMQの「即時削除」). Age
			// is omitted when zero because BullMQ treats 0 as unset.
			if o.KeepJobs.Count != nil {
				keep["count"] = *o.KeepJobs.Count
			}
			if o.KeepJobs.Age > 0 {
				keep["age"] = o.KeepJobs.Age
			}
		}
		m := map[string]any{
			"token":          o.Token,
			"lockDuration":   o.LockDuration,
			"attempts":       o.Attempts,
			"keepJobs":       keep,
			"maxMetricsSize": "",
		}
		if o.Name != "" {
			m["name"] = o.Name
		}
		if l := o.Limiter; l != nil && l.Max > 0 && l.DurationMs > 0 {
			m["limiter"] = map[string]any{
				"max":      l.Max,
				"duration": l.DurationMs,
			}
		}
		return enc.Encode(m)
	})
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
	// Hot path on the failed-job branch (pairs typically [stacktrace,
	// <stack>]) plus the retry path. Uses the package encoder pool.
	return encodeMsgpack(func(enc *msgpack.Encoder) error {
		return enc.Encode(pairs)
	})
}

var errOddJobFields = errors.New("proto: EncodeJobFields requires an even number of arguments")
