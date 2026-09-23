package proto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DecodeScheduleTemplateOpts reads back what the Lua wrote to the
// scheduler HASH, which may have come from BullMQ TS.
//
// **個別のフィールドを知ろうとせず丸ごと持ち越す。** mkq の worker は
// scheduler HASH から per-iteration opts を組み直すので、知っている
// フィールドだけを写すと知らないものが毎回落ちる。`attempts` が落ちれば
// 2 本目以降は再試行しなくなり、`priority` が落ちれば置き場所が
// prioritized ZSET から wait へ変わる。
func TestDecodeScheduleTemplateOpts_CarriesEverything(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		want    map[string]any
		wantErr bool
	}{
		{name: "empty", raw: "", want: nil},
		{name: "empty object", raw: "{}", want: nil},
		{
			name: "retention in both wire forms",
			raw:  `{"removeOnComplete":{"age":604800},"removeOnFail":50}`,
			want: map[string]any{
				"removeOnComplete": map[string]any{"age": int64(604800)},
				"removeOnFail":     int64(50),
			},
		},
		{
			name: "fields mkq has no option for ride along",
			raw:  `{"attempts":3,"priority":5,"backoff":{"type":"exponential","delay":1000}}`,
			want: map[string]any{
				"attempts": int64(3),
				"priority": int64(5),
				"backoff":  map[string]any{"type": "exponential", "delay": int64(1000)},
			},
		},
		{
			name: "an option mkq has never heard of is still carried",
			raw:  `{"someFutureBullMQOption":{"nested":[1,2]}}`,
			want: map[string]any{
				"someFutureBullMQOption": map[string]any{"nested": []any{int64(1), int64(2)}},
			},
		},
		{
			name:    "broken JSON drops the template but is reported",
			raw:     `{not json`,
			want:    nil,
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeScheduleTemplateOpts(tc.raw)
			if tc.wantErr {
				require.Error(t, err, "壊れた template は呼び出し側に見せる")
			} else {
				require.NoError(t, err)
			}
			if tc.want == nil {
				assert.Empty(t, got.Extra)
				return
			}
			assert.Equal(t, tc.want, got.Extra)
		})
	}
}

// TestDecodeScheduleTemplateOpts_KeepsIntegersIntegral is the trap that
// makes the whole-map passthrough workable at all.
//
// **素の `json.Unmarshal` は数値をすべて float64 にする。** それを msgpack に
// 載せると Lua が double として受け取り、cjson が書き戻すときに `attempts: 3`
// が `3.0` になりうる。BullMQ TS が書く形と変わってしまうので、整数は整数の
// まま持ち越す。小数は小数のまま。
func TestDecodeScheduleTemplateOpts_KeepsIntegersIntegral(t *testing.T) {
	got, err := DecodeScheduleTemplateOpts(
		`{"attempts":3,"jitter":0.25,"big":9007199254740993,"nested":{"delay":1000}}`)
	require.NoError(t, err)

	require.IsType(t, int64(0), got.Extra["attempts"], "an integer must not become a float")
	assert.EqualValues(t, 3, got.Extra["attempts"])

	require.IsType(t, float64(0), got.Extra["jitter"], "a fractional value stays fractional")
	assert.InDelta(t, 0.25, got.Extra["jitter"], 1e-9)

	// float64 では表現できない大きさの整数も、int64 に収まるなら保つ。
	require.IsType(t, int64(0), got.Extra["big"])
	assert.EqualValues(t, 9007199254740993, got.Extra["big"])

	nested, ok := got.Extra["nested"].(map[string]any)
	require.True(t, ok)
	require.IsType(t, int64(0), nested["delay"], "nested integers too")
}

// ScheduleOption で明示された retention は、持ち越した値より優先する。
// 同じ schedule を新しい retention で upsert し直したのに古い値が
// 生き残る、ということが無いように。
func TestScheduleTemplateOpts_TypedRetentionOverridesCarried(t *testing.T) {
	five := 5
	in := ScheduleTemplateOpts{
		RemoveOnComplete: &RetentionLimit{Count: &five},
		Extra: map[string]any{
			"removeOnComplete": int64(999),
			"attempts":         int64(3),
		},
	}
	m := in.toMap()
	assert.Equal(t, 5, m["removeOnComplete"], "the explicit option wins")
	assert.Equal(t, int64(3), m["attempts"], "unrelated carried fields are untouched")
}

// What mkq encodes must survive the trip back, otherwise the template
// drifts a little on every reschedule.
func TestScheduleTemplateOpts_RoundTrip(t *testing.T) {
	in := ScheduleTemplateOpts{
		RemoveOnComplete: &RetentionLimit{AgeSeconds: ptr(604800)},
		RemoveOnFail:     &RetentionLimit{Count: ptr(50)},
		Extra:            map[string]any{"attempts": int64(3)},
	}
	// Lua 側は msgpack で受け取った template を cjson.encode して HASH に
	// 書く。ここではその JSON 化を stdlib で代用している。
	encoded, err := json.Marshal(in.toMap())
	require.NoError(t, err)

	got, err := DecodeScheduleTemplateOpts(string(encoded))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"removeOnComplete": map[string]any{"age": int64(604800)},
		"removeOnFail":     int64(50),
		"attempts":         int64(3),
	}, got.Extra)

	// 2 周目で形が変わらないこと。持ち越しが毎回ずれていくと、何周か
	// したあとで別物になる。
	again, err := json.Marshal(got.toMap())
	require.NoError(t, err)
	assert.JSONEq(t, string(encoded), string(again))
}

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

func ptr[T any](v T) *T { return &v }
