package proto

import (
	"github.com/vmihailenco/msgpack/v5"
)

// ScheduleOpts is the BullMQ schedule template — the value
// addJobScheduler-11.lua reads as ARGV[2] (cmsgpack-unpacked).
//
// EveryMs and Pattern are mutually exclusive: every-mode lets Lua
// recompute the next millis via getJobSchedulerEveryNextMillis,
// while pattern-mode passes a Go-computed nextMillis in ARGV[1]
// and Lua uses it verbatim.
type ScheduleOpts struct {
	// Name is the BullMQ job name written to each created instance
	// (Job.name). Defaults to the queue name.
	Name string
	// EveryMs is the fixed interval in milliseconds. Required for
	// every-mode; the vendored Lua's getJobSchedulerEveryNextMillis
	// computes the next fire time from this.
	EveryMs int64
	// Pattern is the cron expression (5-field) for pattern-mode
	// schedules. Mutually exclusive with EveryMs.
	Pattern string
	// TZ is the IANA timezone name for pattern-mode (e.g.
	// "Asia/Tokyo"). Empty = local time. Pattern-mode only.
	TZ string
	// StartDate (optional) is the earliest absolute ms timestamp at
	// which the first instance fires. 0 = unset (fire as soon as
	// possible).
	StartDate int64
	// EndDate (optional) is the absolute ms timestamp after which no
	// new instances are scheduled. 0 = unset (run forever).
	EndDate int64
	// Limit (optional) caps the total number of fires. 0 = unset.
	Limit int
}

// EncodeScheduleOpts produces the ARGV[2] payload for
// addJobScheduler-11.lua.
func EncodeScheduleOpts(o ScheduleOpts) ([]byte, error) {
	m := map[string]any{"name": o.Name}
	if o.EveryMs > 0 {
		m["every"] = o.EveryMs
	}
	if o.Pattern != "" {
		m["pattern"] = o.Pattern
	}
	if o.TZ != "" {
		m["tz"] = o.TZ
	}
	if o.StartDate > 0 {
		m["startDate"] = o.StartDate
	}
	if o.EndDate > 0 {
		m["endDate"] = o.EndDate
	}
	if o.Limit > 0 {
		m["limit"] = o.Limit
	}
	return msgpack.Marshal(m)
}

// EncodeScheduleTemplateOpts encodes the per-iteration job opts
// stored in the schedule template HASH (`opts` field) and used by
// addJobFromScheduler when creating each new instance. mkq's first
// scheduler PR ships an empty template; future PRs may carry retry
// / backoff / retention settings.
//
// Returns the empty msgpack map when no fields are set, which the
// Lua treats as "no per-instance overrides".
func EncodeScheduleTemplateOpts() ([]byte, error) {
	return msgpack.Marshal(map[string]any{})
}

// EncodeScheduleDelayedOpts builds ARGV[6] for addJobScheduler-11 (and
// ARGV[4] for updateJobScheduler-12): the per-iteration job opts
// merged with `repeat: {every, ...}`. BullMQ TS Worker reads this
// nested `repeat` block to invoke its own upsertJobScheduler when it
// finishes the iteration; without `every` here, foreign workers
// crash on `Cannot destructure property 'every' of 'repeatOpts' as
// it is undefined` (BullMQ JobScheduler.upsertJobScheduler).
//
// We do not populate `count` from the Go side: BullMQ TS computes it
// from the schedule HASH `ic` field at re-upsert time, and both
// addJobScheduler-11 and updateJobScheduler-12 advance `ic` inside
// the script — passing a stale Go-computed count would race.
func EncodeScheduleDelayedOpts(o ScheduleOpts) ([]byte, error) {
	repeat := map[string]any{}
	if o.EveryMs > 0 {
		repeat["every"] = o.EveryMs
	}
	if o.Pattern != "" {
		repeat["pattern"] = o.Pattern
	}
	if o.TZ != "" {
		repeat["tz"] = o.TZ
	}
	if o.StartDate > 0 {
		repeat["startDate"] = o.StartDate
	}
	if o.EndDate > 0 {
		repeat["endDate"] = o.EndDate
	}
	if o.Limit > 0 {
		repeat["limit"] = o.Limit
	}
	return msgpack.Marshal(map[string]any{"repeat": repeat})
}
