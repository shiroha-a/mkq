package proto

import (
	"encoding/json"

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

// ScheduleTemplateOpts is the job-level template a schedule carries:
// the options every iteration inherits, as opposed to ScheduleOpts,
// which describes when the iterations fire.
//
// **BullMQ 側ではこれが scheduler HASH の `opts` field になる。**
// TS の Worker は再スケジュールのときにそこを読んで次の iteration の
// opts を組み立てるので (`job-scheduler.ts` の upsertJobScheduler:
// `{...opts, repeat: filteredRepeatOpts}`)、ここに載せたものは foreign
// worker が回しても引き継がれる。
type ScheduleTemplateOpts struct {
	// RemoveOnComplete / RemoveOnFail bound how long each iteration's
	// terminal record survives. nil leaves the field out, which is
	// BullMQ's "keep forever".
	RemoveOnComplete *RetentionLimit
	RemoveOnFail     *RetentionLimit
}

// IsZero reports whether the template carries nothing, so callers can
// keep writing the empty map the Lua reads as "no overrides".
func (t ScheduleTemplateOpts) IsZero() bool {
	return t.RemoveOnComplete == nil && t.RemoveOnFail == nil
}

func (t ScheduleTemplateOpts) toMap() map[string]any {
	m := map[string]any{}
	if v := encodeRetentionLimit(t.RemoveOnComplete); v != nil {
		m["removeOnComplete"] = v
	}
	if v := encodeRetentionLimit(t.RemoveOnFail); v != nil {
		m["removeOnFail"] = v
	}
	return m
}

// EncodeScheduleTemplateOpts encodes the per-iteration job opts
// stored in the schedule template HASH (`opts` field) and used by
// addJobFromScheduler when creating each new instance.
//
// Returns the empty msgpack map when no fields are set, which the
// Lua treats as "no per-instance overrides".
func EncodeScheduleTemplateOpts(t ScheduleTemplateOpts) ([]byte, error) {
	return msgpack.Marshal(t.toMap())
}

// DecodeScheduleTemplateOpts reads back the JSON the Lua wrote to the
// scheduler HASH `opts` field.
//
// **worker が次の iteration を積むときに要る。** mkq の worker は
// scheduler HASH から every / pattern 等を読んで per-iteration opts を
// 組み直すので、template に載せた retention をここで拾わないと 2 回目
// 以降の iteration だけ retention が落ちる。
//
// Unknown fields are ignored: this only needs the parts mkq puts back
// on the next iteration, and a foreign writer may have stored more.
func DecodeScheduleTemplateOpts(raw string) ScheduleTemplateOpts {
	if raw == "" || raw == "{}" {
		return ScheduleTemplateOpts{}
	}
	var wire struct {
		RemoveOnComplete json.RawMessage `json:"removeOnComplete"`
		RemoveOnFail     json.RawMessage `json:"removeOnFail"`
	}
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		// 壊れた opts で再スケジュールを止めるほうが害が大きい。
		// retention を落として続ける。
		return ScheduleTemplateOpts{}
	}
	return ScheduleTemplateOpts{
		RemoveOnComplete: decodeRetentionLimit(wire.RemoveOnComplete),
		RemoveOnFail:     decodeRetentionLimit(wire.RemoveOnFail),
	}
}

// decodeRetentionLimit accepts both wire forms BullMQ persists: a bare
// number (count shorthand) and a {count?, age?} object.
func decodeRetentionLimit(raw json.RawMessage) *RetentionLimit {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return &RetentionLimit{Count: &n}
	}
	var obj struct {
		Count *int `json:"count"`
		Age   *int `json:"age"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	if obj.Count == nil && obj.Age == nil {
		return nil
	}
	return &RetentionLimit{Count: obj.Count, AgeSeconds: obj.Age}
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
func EncodeScheduleDelayedOpts(o ScheduleOpts, t ScheduleTemplateOpts) ([]byte, error) {
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
	// **BullMQ TS と同じ組み立て方にする。** あちらは
	// `{...templateOpts, repeat: filteredRepeatOpts}` を per-iteration の
	// opts にしている (`job-scheduler.ts` の getNextJobOpts)。template を
	// 展開せず repeat だけ渡すと、scheduler 経由の job にだけ retention が
	// 載らない。
	m := t.toMap()
	m["repeat"] = repeat
	return msgpack.Marshal(m)
}
