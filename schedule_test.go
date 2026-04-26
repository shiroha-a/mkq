package mkq_test

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// TestSchedule_EveryFires confirms that a fixed-interval schedule
// produces multiple iterations driven by the worker's auto-reschedule
// path. The first instance is queued by UpsertScheduleEvery; subsequent
// instances are queued by updateJobScheduler-12 after each fire
// completes.
func TestSchedule_EveryFires(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const interval = 200 * time.Millisecond
	require.NoError(t, queue.UpsertScheduleEvery(ctx, "ticker", interval, testPayload{Inbox: "tick"}))

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		// 周期 job の jobID は repeat:<scheduleID>:<millis> 形式。
		assert.True(t, strings.HasPrefix(j.ID, "repeat:ticker:"), "unexpected job id %q", j.ID)
		seen.Add(1)
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		return seen.Load() >= 3
	})
}

// TestSchedule_LimitStops confirms that WithScheduleLimit caps total
// iterations: after the cap the worker's reschedule path short-circuits
// (the schedule HASH still exists but `ic >= limit` blocks the
// updateJobScheduler call).
func TestSchedule_LimitStops(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const limit = 3
	require.NoError(t, queue.UpsertScheduleEvery(ctx,
		"capped", 100*time.Millisecond, testPayload{},
		mkq.WithScheduleLimit(limit),
	))

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		seen.Add(1)
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer worker.Stop(context.Background())

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		return seen.Load() >= int64(limit)
	})

	// 数 iteration 分以上余裕を持って待ち、cap 超過がないことを確認。
	time.Sleep(500 * time.Millisecond)
	assert.LessOrEqual(t, seen.Load(), int64(limit), "schedule must not fire past limit")

	rdb := rawClient(t)
	ic, err := rdb.HGet(ctx, prefix+":tick:repeat:capped", "ic").Result()
	require.NoError(t, err)
	icN, _ := strconv.Atoi(ic)
	assert.GreaterOrEqual(t, icN, limit, "ic counter must reach limit")
}

// TestSchedule_RemoveStops confirms that RemoveSchedule prevents
// further iterations: the in-flight job (if any) finishes normally and
// the worker's re-upsert path no-ops because the schedule HASH is gone.
func TestSchedule_RemoveStops(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, "drop", 100*time.Millisecond, testPayload{}))

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		seen.Add(1)
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer worker.Stop(context.Background())

	// 少なくとも 1 回は動かす。
	waitFor(t, ctx, 30*time.Millisecond, func() bool { return seen.Load() >= 1 })

	require.NoError(t, queue.RemoveSchedule(ctx, "drop"))
	atSnapshot := seen.Load()

	// 数 iteration 分待つ。 RemoveSchedule 後は ic が増えないはず。
	time.Sleep(500 * time.Millisecond)
	// in-flight が 1 つ通る可能性があるので +1 まで許容。
	assert.LessOrEqual(t, seen.Load(), atSnapshot+1, "no further fires after RemoveSchedule")

	rdb := rawClient(t)
	exists, err := rdb.Exists(ctx, prefix+":tick:repeat:drop").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, exists, "schedule HASH must be removed")
}

// TestSchedule_OverrideUpsert confirms that re-calling
// UpsertScheduleEvery with the same scheduleID replaces the schedule
// (the addJobScheduler-11 lua's "override" semantics: drops the
// pending delayed instance and re-stores the template).
func TestSchedule_OverrideUpsert(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 最初は遅い周期で登録、すぐ速い周期に置き換える。
	require.NoError(t, queue.UpsertScheduleEvery(ctx, "swap", 5*time.Second, testPayload{Inbox: "slow"}))
	require.NoError(t, queue.UpsertScheduleEvery(ctx, "swap", 100*time.Millisecond, testPayload{Inbox: "fast"}))

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		// 上書き後の data が反映されていること。
		assert.Equal(t, "fast", j.Data.Inbox)
		seen.Add(1)
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer worker.Stop(context.Background())

	// 速い周期なら 1 秒以内に 2 回以上発火するはず。slow のままなら
	// 1 秒では 0 回しか出ない。
	waitFor(t, ctx, 50*time.Millisecond, func() bool { return seen.Load() >= 2 })
}
