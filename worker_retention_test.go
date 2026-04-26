package mkq_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// TestWorker_Retention_KeepCompletedTrims verifies that
// WithKeepCompleted(N) bounds the completed ZSET to the most recent N
// entries via BullMQ's removeJobsByMaxCount.
func TestWorker_Retention_KeepCompletedTrims(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const total = 110
	const keep = 100
	for i := range total {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)},
			mkq.WithKeepCompleted(keep),
		)
		require.NoError(t, err)
	}

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	},
		mkq.WithConcurrency(4),
		mkq.WithIdlePollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	defer worker.Stop(context.Background())

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// Wait for the wait list to drain and the trim to settle.
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		waitLen, _ := rdb.LLen(ctx, base+"wait").Result()
		activeLen, _ := rdb.LLen(ctx, base+"active").Result()
		completedLen, _ := rdb.ZCard(ctx, base+"completed").Result()
		// 全部処理し終わって、completed が trim 後の値で安定したか。
		return waitLen == 0 && activeLen == 0 && completedLen == int64(keep)
	})

	got, err := rdb.ZCard(ctx, base+"completed").Result()
	require.NoError(t, err)
	assert.EqualValues(t, keep, got, "completed ZSET must be trimmed to keep")
}

// TestWorker_Retention_KeepCompletedZeroRemovesJob verifies that
// WithKeepCompleted(0) (BullMQ removeOnComplete=true) deletes the job
// HASH outright instead of putting it into the completed ZSET.
func TestWorker_Retention_KeepCompletedZeroRemovesJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{}, mkq.WithKeepCompleted(0))
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer worker.Stop(context.Background())

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		waitLen, _ := rdb.LLen(ctx, base+"wait").Result()
		activeLen, _ := rdb.LLen(ctx, base+"active").Result()
		exists, _ := rdb.Exists(ctx, base+job.ID).Result()
		// HASH が消滅し、queue keys も空になるまで待つ。
		return waitLen == 0 && activeLen == 0 && exists == 0
	})

	completedLen, _ := rdb.ZCard(ctx, base+"completed").Result()
	assert.Zero(t, completedLen, "completed ZSET must be empty when keep=0")
	exists, _ := rdb.Exists(ctx, base+job.ID).Result()
	assert.EqualValues(t, 0, exists, "job HASH must be removed when keep=0")
}

// TestWorker_Retention_DefaultKeepsAll regression-pins the BullMQ
// default: when neither WithKeepCompleted nor WithKeepFailed is set,
// finished jobs accumulate in their respective ZSET indefinitely.
func TestWorker_Retention_DefaultKeepsAll(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const total = 5
	for i := range total {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
	}

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer worker.Stop(context.Background())

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n == int64(total)
	})

	got, err := rdb.ZCard(ctx, base+"completed").Result()
	require.NoError(t, err)
	assert.EqualValues(t, total, got, "default retention must keep all completed jobs")
}

// TestWorker_Retention_KeepFailedTrims is the failure-side analogue
// of KeepCompletedTrims.
func TestWorker_Retention_KeepFailedTrims(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const total = 5
	const keep = 2
	for i := range total {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)},
			mkq.WithKeepFailed(keep),
		)
		require.NoError(t, err)
	}

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("boom")
	},
		mkq.WithConcurrency(2),
		mkq.WithIdlePollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	defer worker.Stop(context.Background())

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		waitLen, _ := rdb.LLen(ctx, base+"wait").Result()
		activeLen, _ := rdb.LLen(ctx, base+"active").Result()
		failedLen, _ := rdb.ZCard(ctx, base+"failed").Result()
		return waitLen == 0 && activeLen == 0 && failedLen == int64(keep)
	})

	got, err := rdb.ZCard(ctx, base+"failed").Result()
	require.NoError(t, err)
	assert.EqualValues(t, keep, got, "failed ZSET must be trimmed to keep")
}
