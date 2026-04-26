package mkq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shiroha-a/mkq/internal/lua"
	"github.com/shiroha-a/mkq/internal/proto"
)

// scheduleConfig is the typed-option bag for UpsertScheduleEvery.
// Public callers populate it via WithSchedule* options; internal
// re-upserts (worker auto-schedule) construct it directly.
type scheduleConfig struct {
	limit     int
	endDate   time.Time
	startDate time.Time
}

// ScheduleOption customises an UpsertScheduleEvery call.
type ScheduleOption func(*scheduleConfig)

// WithScheduleLimit caps the total number of fires for a schedule.
// Once reached, no further instances are created. n <= 0 disables
// the cap (BullMQ default).
func WithScheduleLimit(n int) ScheduleOption {
	return func(c *scheduleConfig) { c.limit = n }
}

// WithScheduleEndDate sets the absolute time after which no new
// instances are scheduled. A zero time disables the bound.
func WithScheduleEndDate(t time.Time) ScheduleOption {
	return func(c *scheduleConfig) { c.endDate = t }
}

// WithScheduleStartDate sets the earliest time at which the first
// instance may fire. A zero time means "fire as soon as possible
// after upsert".
func WithScheduleStartDate(t time.Time) ScheduleOption {
	return func(c *scheduleConfig) { c.startDate = t }
}

// UpsertScheduleEvery registers (or replaces) a fixed-interval
// recurring schedule for q. The first instance is queued immediately
// (subject to WithScheduleStartDate); subsequent instances are
// queued by the worker after each fire completes.
//
// scheduleID identifies the schedule across iterations; calling
// UpsertScheduleEvery again with the same id replaces the schedule
// in place (override-true semantics).
//
// payload is JSON-encoded into the BullMQ template `data` field and
// reused for every instance.
func (q *Queue[T]) UpsertScheduleEvery(
	ctx context.Context,
	scheduleID string,
	every time.Duration,
	payload T,
	opts ...ScheduleOption,
) error {
	if scheduleID == "" {
		return fmt.Errorf("mkq: scheduleID must be non-empty")
	}
	if every <= 0 {
		return fmt.Errorf("mkq: every must be positive")
	}

	cfg := scheduleConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	dataJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("mkq: marshal schedule payload: %w", err)
	}

	return q.upsertSchedule(ctx, scheduleID, every.Milliseconds(), cfg, string(dataJSON))
}

// upsertSchedule is the wire-level call shared by UpsertScheduleEvery
// (public) and the worker's auto-rescheduling path.
func (q *Queue[T]) upsertSchedule(
	ctx context.Context,
	scheduleID string,
	everyMs int64,
	cfg scheduleConfig,
	dataJSON string,
) error {
	now := time.Now().UnixMilli()

	scheduleOpts := proto.ScheduleOpts{
		Name:    q.name,
		EveryMs: everyMs,
		Limit:   cfg.limit,
	}
	if !cfg.startDate.IsZero() {
		scheduleOpts.StartDate = cfg.startDate.UnixMilli()
	}
	if !cfg.endDate.IsZero() {
		scheduleOpts.EndDate = cfg.endDate.UnixMilli()
	}
	scheduleOptsBytes, err := proto.EncodeScheduleOpts(scheduleOpts)
	if err != nil {
		return fmt.Errorf("mkq: encode schedule opts: %w", err)
	}
	templateOptsBytes, err := proto.EncodeScheduleTemplateOpts()
	if err != nil {
		return fmt.Errorf("mkq: encode template opts: %w", err)
	}
	// 各 iteration の job HASH に書かれる opts。BullMQ TS Worker は
	// 自前で再スケジュールするとき opts.repeat.every を参照するので、
	// foreign worker と互換にするため repeat ブロックを必ず埋める。
	delayedOptsBytes, err := proto.EncodeScheduleDelayedOpts(scheduleOpts)
	if err != nil {
		return fmt.Errorf("mkq: encode delayed opts: %w", err)
	}

	res, err := q.client.scripts.Run(
		ctx,
		lua.AddJobScheduler,
		q.scheduleKeys(),
		"0", // ARGV[1] nextMillis: every-mode は lua が再計算
		scheduleOptsBytes,
		scheduleID,
		dataJSON,
		templateOptsBytes,
		delayedOptsBytes,
		now,
		q.keys.Base(),
		"", // ARGV[9] producer key (optional)
	)
	if err != nil {
		return fmt.Errorf("mkq: addJobScheduler: %w", err)
	}
	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("mkq: addJobScheduler returned error code %d", code)
	}
	return nil
}

// RemoveSchedule deletes a schedule. The currently-queued delayed
// instance (if any) is also dropped, but already-running instances
// are not interrupted.
func (q *Queue[T]) RemoveSchedule(ctx context.Context, scheduleID string) error {
	if scheduleID == "" {
		return fmt.Errorf("mkq: scheduleID must be non-empty")
	}

	scheduleKey := q.keys.Schedule(scheduleID)
	// Drop the schedule template HASH and the repeat ZSET entry.
	// We don't reach into delayed/active for the in-flight instance
	// — those finalise normally and the worker's re-upsert path
	// short-circuits because the schedule template HASH is gone.
	pipe := q.client.rdb.Pipeline()
	pipe.Del(ctx, scheduleKey)
	pipe.ZRem(ctx, q.keys.Repeat(), scheduleID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("mkq: remove schedule %s: %w", scheduleID, err)
	}
	return nil
}

// scheduleKeys assembles KEYS[1..11] for addJobScheduler-11.lua.
//
//	KEYS[1]  repeat       KEYS[2]  delayed   KEYS[3]  wait
//	KEYS[4]  paused       KEYS[5]  meta      KEYS[6]  prioritized
//	KEYS[7]  marker       KEYS[8]  id        KEYS[9]  events
//	KEYS[10] pc           KEYS[11] active
func (q *Queue[T]) scheduleKeys() []string {
	return []string{
		q.keys.Repeat(),
		q.keys.Delayed(),
		q.keys.Wait(),
		q.keys.Paused(),
		q.keys.Meta(),
		q.keys.Prioritized(),
		q.keys.Marker(),
		q.keys.ID(),
		q.keys.Events(),
		q.keys.PriorityCounter(),
		q.keys.Active(),
	}
}
