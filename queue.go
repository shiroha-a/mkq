package mkq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/shiroha-a/mkq/internal/keys"
	"github.com/shiroha-a/mkq/internal/lua"
	"github.com/shiroha-a/mkq/internal/proto"
)

// Queue is a typed handle to a BullMQ-compatible queue.
//
// Construct one via Define[T]; reuse the returned pointer for the
// lifetime of the Client.
type Queue[T any] struct {
	client *Client
	name   string
	keys   keys.Builder
}

// Define returns a Queue handle for name. Calling Define twice with the
// same name is safe; both handles operate on identical Redis state.
func Define[T any](c *Client, name string, _ ...QueueOption) *Queue[T] {
	return &Queue[T]{
		client: c,
		name:   name,
		keys:   keys.New(c.keyPrefix, name),
	}
}

// Name returns the queue name as passed to Define.
func (q *Queue[T]) Name() string { return q.name }

// Add enqueues a new job with the given payload and options. The
// returned Job mirrors the wire-level state Lua just wrote to Redis.
//
// Add chooses one of three vendored Lua scripts based on the supplied
// options:
//
//   - WithDelay rounds to >= 1ms → addDelayedJob-6
//   - WithPriority > 0           → addPrioritizedJob-9
//   - otherwise                  → addStandardJob-9
//
// Combining WithPriority and WithDelay returns ErrPriorityWithDelay.
func (q *Queue[T]) Add(ctx context.Context, payload T, opts ...AddOption) (*Job[T], error) {
	cfg := addConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	delayMs := cfg.delay.Milliseconds()
	if delayMs > 0 && cfg.priority > 0 {
		return nil, ErrPriorityWithDelay
	}

	dataJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("mkq: marshal payload: %w", err)
	}

	timestampMs := time.Now().UnixMilli()

	args := proto.AddArgs{
		Prefix:    q.keys.Base(),
		CustomID:  cfg.jobID,
		Name:      q.name,
		Timestamp: timestampMs,
	}
	argsBytes, err := proto.EncodeAddArgs(args)
	if err != nil {
		return nil, fmt.Errorf("mkq: encode args: %w", err)
	}

	optsBytes, err := proto.EncodeAddOpts(toProtoOpts(cfg, delayMs))
	if err != nil {
		return nil, fmt.Errorf("mkq: encode opts: %w", err)
	}

	scriptName, ks := q.dispatch(delayMs, cfg.priority)
	res, err := q.client.scripts.Run(ctx, scriptName, ks, argsBytes, dataJSON, optsBytes)
	if err != nil {
		return nil, fmt.Errorf("mkq: %s: %w", scriptName, err)
	}

	jobID, err := parseAddResult(res)
	if err != nil {
		return nil, err
	}

	return &Job[T]{
		ID:        jobID,
		Name:      q.name,
		Data:      payload,
		Timestamp: time.UnixMilli(timestampMs),
	}, nil
}

// dispatch picks the BullMQ Lua entry point and assembles its KEYS list
// per the directives at the top of each .lua file. The slice ordering
// is load-bearing — Lua references KEYS[N] positionally.
//
// delayMs / priority are passed in already-coerced (vs raw cfg) so the
// rounding rules around sub-ms WithDelay live in one place: the caller.
func (q *Queue[T]) dispatch(delayMs int64, priority uint32) (lua.ScriptName, []string) {
	switch {
	case delayMs > 0:
		// addDelayedJob-6 KEYS:
		//   1 marker, 2 meta, 3 id, 4 delayed, 5 completed, 6 events
		return lua.AddDelayedJob, []string{
			q.keys.Marker(),
			q.keys.Meta(),
			q.keys.ID(),
			q.keys.Delayed(),
			q.keys.Completed(),
			q.keys.Events(),
		}
	case priority > 0:
		// addPrioritizedJob-9 KEYS:
		//   1 marker, 2 meta, 3 id, 4 prioritized, 5 delayed,
		//   6 completed, 7 active, 8 events, 9 pc
		return lua.AddPrioritizedJob, []string{
			q.keys.Marker(),
			q.keys.Meta(),
			q.keys.ID(),
			q.keys.Prioritized(),
			q.keys.Delayed(),
			q.keys.Completed(),
			q.keys.Active(),
			q.keys.Events(),
			q.keys.PriorityCounter(),
		}
	default:
		// addStandardJob-9 KEYS:
		//   1 wait, 2 paused, 3 meta, 4 id, 5 completed,
		//   6 delayed, 7 active, 8 events, 9 marker
		return lua.AddStandardJob, []string{
			q.keys.Wait(),
			q.keys.Paused(),
			q.keys.Meta(),
			q.keys.ID(),
			q.keys.Completed(),
			q.keys.Delayed(),
			q.keys.Active(),
			q.keys.Events(),
			q.keys.Marker(),
		}
	}
}

// toProtoOpts adapts the public addConfig to the wire-level proto.AddOpts.
//
// delayMs is the already-coerced millisecond offset (matching BullMQ's
// opts.delay semantics): the vendored Lua adds it to args[4] timestamp
// to derive the absolute delayed target.
func toProtoOpts(cfg addConfig, delayMs int64) proto.AddOpts {
	o := proto.AddOpts{
		Delay:            delayMs,
		Priority:         cfg.priority,
		Attempts:         cfg.attempts,
		Lifo:             cfg.lifo,
		RemoveOnComplete: buildRetentionLimit(cfg.keepCompleted, cfg.keepCompletedAge),
		RemoveOnFail:     buildRetentionLimit(cfg.keepFailed, cfg.keepFailedAge),
	}
	if cfg.backoff != nil {
		o.Backoff = &proto.Backoff{
			Type:  cfg.backoff.Type,
			Delay: cfg.backoff.Delay.Milliseconds(),
		}
	}
	return o
}

// buildRetentionLimit folds the per-Add WithKeep* options into the
// proto wire shape. nil result lets the encoder skip the key entirely.
func buildRetentionLimit(count *int, age *time.Duration) *proto.RetentionLimit {
	if count == nil && age == nil {
		return nil
	}
	rl := &proto.RetentionLimit{Count: count}
	if age != nil {
		// 1-second resolution matches BullMQ. Sub-second values
		// truncate; negatives are passed through to mirror BullMQ's
		// "no validation" stance for `removeOnComplete: { age: -1 }`.
		secs := int(age.Seconds())
		rl.AgeSeconds = &secs
	}
	return rl
}

// parseAddResult coerces the polymorphic EVALSHA return into a job id
// string. BullMQ Lua returns the id as a string by appending `""`.
// Negative integer returns are reserved for error codes (e.g. -5 for
// "missing parent key").
func parseAddResult(res any) (string, error) {
	switch v := res.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int64:
		if v < 0 {
			return "", fmt.Errorf("mkq: lua add returned error code %d", v)
		}
		return strconv.FormatInt(v, 10), nil
	default:
		return "", fmt.Errorf("mkq: unexpected lua add result %T: %v", res, res)
	}
}
