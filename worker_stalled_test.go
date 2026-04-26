package mkq_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// TestWorker_Stalled_RecoversFromDeadWorker simulates a worker that
// acquires a job and never heartbeats (lockDuration is shorter than
// the handler's "work"). A second worker, with stalled detection
// fast-cycling, must reclaim the job and run it to completion on
// the live worker.
func TestWorker_Stalled_RecoversFromDeadWorker(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "stalled"})
	require.NoError(t, err)

	// "Dead" worker: short lock + handler that never returns. The
	// handler ctx will be cancelled by either heartbeat-failure or
	// Worker.Stop, but importantly its lock token will not be
	// renewed past lockDuration.
	deadHandlerEntered := make(chan struct{}, 1)
	deadWorker, err := mkq.Process(queue, func(ctx context.Context, _ *mkq.Job[testPayload]) (any, error) {
		select {
		case deadHandlerEntered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	},
		// Heartbeat clamps to >= 1s, so a sub-second lockDuration
		// guarantees the lock TTL expires before the heartbeat ever
		// fires — mimicking a worker that lost its Redis connection
		// while still owning the active-list slot.
		mkq.WithLockDuration(400*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
		// Disable stalled detection on the dead worker so it can't
		// reclaim its own job.
		mkq.WithStalledInterval(0),
	)
	require.NoError(t, err)

	// Wait until the dead worker has the job in active.
	receiveOrFail(t, ctx, deadHandlerEntered)

	// Live worker: short stalled interval so the test doesn't drag.
	var liveRan atomic.Int64
	liveWorker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		liveRan.Add(1)
		return nil, nil
	},
		mkq.WithLockDuration(5*time.Second),
		mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithStalledInterval(500*time.Millisecond),
	)
	require.NoError(t, err)
	defer liveWorker.Stop(context.Background())
	defer deadWorker.Stop(context.Background())

	// Stalled detection in BullMQ requires two ticks: the first
	// adds the dead worker's active job to the stalled SET, the
	// second observes the missing lock and re-enqueues. Plus the
	// 2s lockDuration must elapse. Allow a generous wait.
	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 100*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})

	assert.GreaterOrEqual(t, liveRan.Load(), int64(1), "live worker must run the recovered job")
}

// TestWorker_Stalled_FailsAfterMaxStalledCount pins BullMQ's
// "stalled too many times" failure path: when a job is reclaimed
// more than maxStalledCount times the Lua writes the `defa` HASH
// field with BullMQ's canonical message.
func TestWorker_Stalled_FailsAfterMaxStalledCount(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	// Workers that grab the job, hold it past the lock TTL, then
	// release ctx (so the test eventually exits).
	hold := func(ctx context.Context, _ *mkq.Job[testPayload]) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	// Use very short lockDuration so each "stall" finishes quickly.
	// Two workers with stalled detection so they keep re-acquiring
	// each other's expired-lock job.
	wA, err := mkq.Process(queue, hold,
		mkq.WithLockDuration(500*time.Millisecond),
		mkq.WithStalledInterval(300*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithMaxStalledCount(1),
	)
	require.NoError(t, err)
	wB, err := mkq.Process(queue, hold,
		mkq.WithLockDuration(500*time.Millisecond),
		mkq.WithStalledInterval(300*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithMaxStalledCount(1),
	)
	require.NoError(t, err)
	defer wA.Stop(context.Background())
	defer wB.Stop(context.Background())

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 200*time.Millisecond, func() bool {
		defa, _ := rdb.HGet(ctx, base+job.ID, "defa").Result()
		return defa != ""
	})

	defa, err := rdb.HGet(ctx, base+job.ID, "defa").Result()
	require.NoError(t, err)
	assert.Equal(t, "job stalled more than allowable limit", defa,
		"BullMQ's canonical default-failed message must be set")
}

// TestWorker_Stalled_HealthyWorkerNeverStalls regression-pins that a
// normally heartbeating worker is not flagged as stalled even with
// aggressive stalled detection.
func TestWorker_Stalled_HealthyWorkerNeverStalls(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	// Handler does real work briefly but well within lockDuration;
	// stalled detection cycles many times during that window.
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		time.Sleep(200 * time.Millisecond)
		return nil, nil
	},
		mkq.WithLockDuration(5*time.Second),
		mkq.WithStalledInterval(50*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer worker.Stop(context.Background())

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})

	// stc must NOT be incremented for a healthy worker. go-redis v9
	// returns redis.Nil when the field is absent, which is the
	// "stc was never written" case we're asserting.
	stc, err := rdb.HGet(ctx, base+job.ID, "stc").Result()
	if err == redis.Nil {
		stc = ""
	} else {
		require.NoError(t, err)
	}
	assert.True(t, stc == "" || stc == "0", "healthy worker must not bump stc, got %q", stc)
}
