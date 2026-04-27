package mkq_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// TestWorker_DelayedDispatch_PreciseWakeup proves the dispatchLoop's
// nextDelayedTs branch interprets the Lua's value as a raw
// millisecond timestamp (not as BullMQ's packed delayed-ZSET score).
// The Lua's getNextDelayedTimestamp already divides the packed
// score by 0x1000 before returning; mkq receives raw ms.
//
// Setup: 5s WithIdlePollInterval (would normally cap awaitMarker)
// + a job with 400ms delay. With correct interpretation, the
// dispatcher sleeps precisely ~400ms and the handler runs in the
// 400-600ms window. If the value were instead the packed score
// (~4096× larger), time.Until would return effectively infinity
// and the cap would force a 1s+ wakeup, blowing through the
// upper bound.
func TestWorker_DelayedDispatch_PreciseWakeup(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entered := make(chan time.Time, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		entered <- time.Now()
		return nil, nil
	},
		mkq.WithIdlePollInterval(5*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	const targetDelay = 400 * time.Millisecond
	addedAt := time.Now()
	_, err = queue.Add(ctx, testPayload{Inbox: "delayed"}, mkq.WithDelay(targetDelay))
	require.NoError(t, err)

	select {
	case at := <-entered:
		elapsed := at.Sub(addedAt)
		assert.GreaterOrEqual(t, elapsed, targetDelay-50*time.Millisecond,
			"handler must not run before the delay (got %v)", elapsed)
		assert.Less(t, elapsed, 800*time.Millisecond,
			"handler must run within ~400ms (precise sleep working); got %v. Larger value means nextDelayedTs is being misinterpreted.", elapsed)
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not run within 3s of Add (delayed dispatch broken)")
	}
}

// TestWorker_MarkerDispatch_BeatsIdleInterval proves the
// marker-based BZPopMin dispatch wakes far faster than a worst-case
// sleep on WithIdlePollInterval. We register a worker with a
// deliberately-large 5s idle ceiling; if the dispatcher were still
// polling, the first job would land near that ceiling. With marker
// dispatch the wakeup is bounded by the Lua's marker push (~ms),
// so the handler entry should fire in well under a second.
func TestWorker_MarkerDispatch_BeatsIdleInterval(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Worker first, then Add. The dispatcher's first BZPopMin call
	// should be blocked when the Add fires; the marker push wakes it
	// within ms despite the 5-second timeout ceiling.
	entered := make(chan time.Time, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		entered <- time.Now()
		return nil, nil
	},
		mkq.WithIdlePollInterval(5*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	// Give the dispatcher a beat to enter its first BZPopMin call.
	time.Sleep(100 * time.Millisecond)

	addedAt := time.Now()
	_, err = queue.Add(ctx, testPayload{Inbox: "fast"})
	require.NoError(t, err)

	select {
	case at := <-entered:
		elapsed := at.Sub(addedAt)
		assert.Less(t, elapsed, 500*time.Millisecond,
			"marker dispatch should wake in well under 500ms (idle interval is 5s); got %v", elapsed)
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not run within 2s of Add (marker dispatch broken?)")
	}
}
