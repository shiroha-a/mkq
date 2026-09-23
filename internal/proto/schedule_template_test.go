package proto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DecodeScheduleTemplateOpts reads what the Lua wrote to the scheduler
// HASH, which may have come from BullMQ TS.
//
// **BullMQ は retention を 2 つの形で書く。** count だけなら数値そのもの
// (`"removeOnFail": 50`)、age が絡めばオブジェクト (`{"age": 604800}`)。
// TS が作った scheduler を mkq の worker が回し続ける構成では、両方とも
// 読めないと 2 本目以降の iteration で retention が落ちる。
func TestDecodeScheduleTemplateOpts_BothWireForms(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want ScheduleTemplateOpts
	}{
		{"empty", "", ScheduleTemplateOpts{}},
		{"empty object", "{}", ScheduleTemplateOpts{}},
		{
			"count shorthand (BullMQ writes a bare number)",
			`{"removeOnComplete":5,"removeOnFail":50}`,
			ScheduleTemplateOpts{
				RemoveOnComplete: &RetentionLimit{Count: ptr(5)},
				RemoveOnFail:     &RetentionLimit{Count: ptr(50)},
			},
		},
		{
			"age object",
			`{"removeOnComplete":{"age":604800}}`,
			ScheduleTemplateOpts{RemoveOnComplete: &RetentionLimit{AgeSeconds: ptr(604800)}},
		},
		{
			"count and age together",
			`{"removeOnComplete":{"count":10,"age":3600}}`,
			ScheduleTemplateOpts{RemoveOnComplete: &RetentionLimit{Count: ptr(10), AgeSeconds: ptr(3600)}},
		},
		{
			"zero count trims immediately and must not be dropped",
			`{"removeOnComplete":0}`,
			ScheduleTemplateOpts{RemoveOnComplete: &RetentionLimit{Count: ptr(0)}},
		},
		{
			"unknown fields are ignored, not fatal",
			`{"removeOnComplete":5,"attempts":3,"backoff":{"type":"exponential"}}`,
			ScheduleTemplateOpts{RemoveOnComplete: &RetentionLimit{Count: ptr(5)}},
		},
		{
			"broken JSON drops retention rather than stopping the reschedule",
			`{not json`,
			ScheduleTemplateOpts{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := DecodeScheduleTemplateOpts(tc.raw)
			assert.Equal(t, tc.want, got)
		})
	}
}

// What mkq encodes must survive a round trip through the wire shape,
// otherwise a template written by mkq and re-read by mkq's own worker
// would drift.
func TestScheduleTemplateOpts_RoundTrip(t *testing.T) {
	in := ScheduleTemplateOpts{
		RemoveOnComplete: &RetentionLimit{AgeSeconds: ptr(604800)},
		RemoveOnFail:     &RetentionLimit{Count: ptr(50)},
	}
	// Lua 側は msgpack で受け取った template を cjson.encode して HASH に
	// 書く。ここではその JSON 化を stdlib で代用している。
	encoded, err := json.Marshal(in.toMap())
	require.NoError(t, err)
	assert.Equal(t, in, DecodeScheduleTemplateOpts(string(encoded)))
}

func ptr[T any](v T) *T { return &v }

// TestEncodeRetentionLimit_DropsZeroAge pins the one case where the two
// readers of the same job disagreed.
//
// **age を 0 で書くと処理系によって意味が正反対になる。** mkq の finish path
// は `Age > 0` のときしか keepJobs に載せないので「刈らない」になるが、
// BullMQ TS の getKeepJobs はオブジェクトをそのまま Lua へ渡すので
// `maxAge = 0` となり、`removeJobsByMaxAge` が now 以前の全件を消す —
// 今完了した job も含めて。秒未満を切り捨てた結果の 0 は書かない。
func TestEncodeRetentionLimit_DropsZeroAge(t *testing.T) {
	zero, five, ten := 0, 5, 10
	for _, tc := range []struct {
		name string
		in   *RetentionLimit
		want any
	}{
		{"nil", nil, nil},
		{"count only takes the number shorthand", &RetentionLimit{Count: &five}, 5},
		{"count zero means remove immediately", &RetentionLimit{Count: &zero}, 0},
		{"positive age takes the object form", &RetentionLimit{AgeSeconds: &ten},
			map[string]any{"age": 10}},
		{"count and age", &RetentionLimit{Count: &five, AgeSeconds: &ten},
			map[string]any{"count": 5, "age": 10}},
		{"zero age alone encodes nothing", &RetentionLimit{AgeSeconds: &zero}, nil},
		{"zero age with a count keeps the count", &RetentionLimit{Count: &five, AgeSeconds: &zero}, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, encodeRetentionLimit(tc.in))
		})
	}
}
