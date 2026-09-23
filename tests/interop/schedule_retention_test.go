//go:build interop

// Retention parity for job schedulers.
//
// BullMQ keeps a scheduler's per-iteration job options on the scheduler
// HASH under `opts`, and its Worker reads that back when it queues the
// next iteration. If mkq wrote a different shape there, a BullMQ TS
// worker sharing the queue would either drop the retention or choke on
// it — and the jobs mkq's own scheduler creates would stop being
// readable as BullMQ scheduled jobs.

package interop_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// nodeUpsertScheduler drives BullMQ TS's own upsertJobScheduler.
func nodeUpsertScheduler(t *testing.T, prefix, queueName, scheduleID string, everyMs int, templateOptsJSON string) {
	t.Helper()
	var got struct {
		JobID string `json:"jobId"`
	}
	runNodeScript(t, "scheduler.js", prefix, queueName, &got,
		"upsert", scheduleID, strconv.Itoa(everyMs), templateOptsJSON)
	require.NotEmpty(t, got.JobID, "BullMQ TS must have queued the first iteration")
}

// TestInterop_ScheduleRetention_TemplateMatchesBullMQ pins mkq's
// scheduler template against the one BullMQ TS writes for the same
// retention, field for field.
func TestInterop_ScheduleRetention_TemplateMatchesBullMQ(t *testing.T) {
	const queueName = "tick"
	const scheduleID = "ticker"
	const everyMs = 3600000

	// BullMQ TS 側
	tsPrefix := uniquePrefix(t)
	nodeUpsertScheduler(t, tsPrefix, queueName, scheduleID, everyMs,
		`{"removeOnComplete":{"age":604800},"removeOnFail":50}`)

	// mkq 側
	mkqPrefix := uniquePrefix(t)
	c := newClient(t, mkqPrefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, scheduleID,
		time.Duration(everyMs)*time.Millisecond, interopPayload{},
		mkq.WithScheduleKeepCompletedAge(7*24*time.Hour),
		mkq.WithScheduleKeepFailed(50),
	))

	rdb := rawClient(t)
	readTemplate := func(prefix string) map[string]any {
		t.Helper()
		raw, err := rdb.HGet(ctx, prefix+":"+queueName+":repeat:"+scheduleID, "opts").Result()
		require.NoError(t, err, "scheduler HASH must carry an opts template")
		var out map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &out))
		return out
	}

	ts := readTemplate(tsPrefix)
	got := readTemplate(mkqPrefix)

	assert.Equal(t, ts["removeOnComplete"], got["removeOnComplete"],
		"removeOnComplete must be written in BullMQ's shape")
	assert.Equal(t, ts["removeOnFail"], got["removeOnFail"],
		"removeOnFail must be written in BullMQ's shape")

	// 参照側が本当に retention を書いていることも確かめる。両方が空なら
	// 上の比較は何も証明しない。
	require.Contains(t, ts, "removeOnComplete",
		"BullMQ TS itself must persist the template retention; otherwise the parity check proves nothing")
	assert.EqualValues(t, 604800, ts["removeOnComplete"].(map[string]any)["age"])
}

// The first iteration's job HASH has to carry the same retention, since
// that is what moveToFinished reads when the job completes.
func TestInterop_ScheduleRetention_IterationOptsMatchBullMQ(t *testing.T) {
	const queueName = "tick"
	const scheduleID = "ticker"
	const everyMs = 3600000
	const template = `{"removeOnComplete":{"age":604800}}`

	tsPrefix := uniquePrefix(t)
	nodeUpsertScheduler(t, tsPrefix, queueName, scheduleID, everyMs, template)

	mkqPrefix := uniquePrefix(t)
	c := newClient(t, mkqPrefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, scheduleID,
		time.Duration(everyMs)*time.Millisecond, interopPayload{},
		mkq.WithScheduleKeepCompletedAge(7*24*time.Hour),
	))

	rdb := rawClient(t)
	firstIterationOpts := func(prefix string) map[string]any {
		t.Helper()
		ids, err := rdb.LRange(ctx, prefix+":"+queueName+":wait", 0, -1).Result()
		require.NoError(t, err)
		require.Len(t, ids, 1, "one iteration must be queued")
		raw, err := rdb.HGet(ctx, prefix+":"+queueName+":"+ids[0], "opts").Result()
		require.NoError(t, err)
		var out map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &out))
		return out
	}

	ts := firstIterationOpts(tsPrefix)
	got := firstIterationOpts(mkqPrefix)

	assert.Equal(t, ts["removeOnComplete"], got["removeOnComplete"],
		"the iteration's own opts must carry the retention in BullMQ's shape")
	require.Contains(t, ts, "removeOnComplete",
		"BullMQ TS itself must put the retention on the iteration")

	// repeat ブロックが retention と共存していること。片方が他方を
	// 上書きしていたら foreign worker が再スケジュールできない。
	require.Contains(t, got, "repeat")
	require.Contains(t, ts, "repeat")
}
