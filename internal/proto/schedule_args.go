package proto

import (
	"encoding/json"
	"fmt"
	"strings"

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

	// Extra carries the template fields mkq has no typed option for —
	// `attempts`, `backoff`, `priority` and anything else a writer put
	// there.
	//
	// **これが無いと、再スケジュールのたびに template が痩せる。** mkq の
	// worker は scheduler HASH から per-iteration opts を組み直すので、
	// 知っているフィールドだけを写すと知らないものが毎回落ちる。BullMQ TS が
	// `attempts: 3` 付きで作った schedule を mkq が回すと 2 本目から再試行
	// しなくなり、`priority` に至っては置き場所 (prioritized ZSET か wait か)
	// まで変わる。BullMQ TS も `{...opts, repeat}` と丸ごと展開している。
	//
	// Populated by DecodeScheduleTemplateOpts on the reschedule path;
	// nil when the template is built from ScheduleOptions.
	Extra map[string]any
}

func (t ScheduleTemplateOpts) toMap() map[string]any {
	m := make(map[string]any, len(t.Extra)+2)
	for k, v := range t.Extra {
		m[k] = v
	}
	// 型付きフィールドが後。ScheduleOption で明示された retention は
	// 持ち越した値より優先する。
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
// scheduler HASH `opts` field, whole.
//
// **worker が次の iteration を積むときに要る。** mkq の worker は
// scheduler HASH から every / pattern 等を読んで per-iteration opts を
// 組み直すので、ここで template を拾わないと 2 回目以降の iteration から
// 落ちる。retention だけでなく `attempts` / `backoff` / `priority` も同じ。
//
// 個別のフィールドを知ろうとせず丸ごと持ち越すのは、mkq が知らない
// オプションを BullMQ が足しても勝手に落とさないため。BullMQ TS も
// `{...opts, repeat}` と展開していて、中身を検査していない。
func DecodeScheduleTemplateOpts(raw string) (ScheduleTemplateOpts, error) {
	if raw == "" || raw == "{}" {
		return ScheduleTemplateOpts{}, nil
	}
	// **UseNumber が要る。** 素の Unmarshal は数値をすべて float64 にするので、
	// msgpack に float64 として載り、Lua を通って `attempts: 3` が `3.0` で
	// 書き戻される。整数は整数のまま持ち越す。
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		// **壊れていても再スケジュールは止めない。** 止めると定期ジョブが
		// 永久に出なくなる。ただし template を落とすと attempts / priority が
		// 静かに消えるので、呼び出し側が気付けるようエラーを返す。
		return ScheduleTemplateOpts{}, fmt.Errorf("decode scheduler template opts: %w", err)
	}
	return ScheduleTemplateOpts{Extra: normaliseJSONNumbers(m).(map[string]any)}, nil
}

// normaliseJSONNumbers turns json.Number back into int64 where the
// value is integral and float64 otherwise, recursively.
func normaliseJSONNumbers(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, vv := range t {
			t[k] = normaliseJSONNumbers(vv)
		}
		return t
	case []any:
		for i, vv := range t {
			t[i] = normaliseJSONNumbers(vv)
		}
		return t
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		// int でも float でも読めない数値は諦めて文字列のまま置く。
		return t.String()
	}
	return v
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
