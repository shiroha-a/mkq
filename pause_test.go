package mkq_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// TestQueue_Pause_MovesWaitToPaused verifies the BullMQ pause semantics:
// the meta.paused flag is set and every job already in wait is parked in
// the paused list (atomic RENAME), so Counts.Paused / IsPaused agree.
func TestQueue_Pause_MovesWaitToPaused(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ids []string
	for range 3 {
		job, err := queue.Add(ctx, testPayload{Inbox: "to-park"})
		require.NoError(t, err)
		ids = append(ids, job.ID)
	}

	require.NoError(t, queue.Pause(ctx))

	paused, err := queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.True(t, paused, "IsPaused must report true after Pause")

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitLen, err := rdb.LLen(ctx, base+"wait").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, waitLen, "wait list must be empty after Pause")

	pausedMembers, err := rdb.LRange(ctx, base+"paused", 0, -1).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, ids, pausedMembers, "all jobs must be parked in paused")

	flag, err := rdb.HGet(ctx, base+"meta", "paused").Result()
	require.NoError(t, err)
	assert.Equal(t, "1", flag, "meta.paused must be set to 1")

	// Pause must DEL the marker (BullMQ pause-7.lua) so idle blocking
	// workers stop being woken to spin against the gate.
	markerCard, err := rdb.ZCard(ctx, base+"marker").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, markerCard, "marker must be deleted by Pause")

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, counts.Paused, "Counts.Paused must reflect parked jobs")
	assert.EqualValues(t, 0, counts.Wait, "Counts.Wait must be zero while paused")
}

// TestQueue_Resume_MovesPausedBackToWait verifies that Resume clears the
// flag, returns parked jobs to wait, and pokes the marker ZSET so a
// blocking worker wakes immediately.
func TestQueue_Resume_MovesPausedBackToWait(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ids []string
	for range 3 {
		job, err := queue.Add(ctx, testPayload{Inbox: "to-resume"})
		require.NoError(t, err)
		ids = append(ids, job.ID)
	}
	require.NoError(t, queue.Pause(ctx))
	require.NoError(t, queue.Resume(ctx))

	paused, err := queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.False(t, paused, "IsPaused must report false after Resume")

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	pausedLen, err := rdb.LLen(ctx, base+"paused").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, pausedLen, "paused list must be empty after Resume")

	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, ids, waitMembers, "all jobs must be back in wait")

	exists, err := rdb.HExists(ctx, base+"meta", "paused").Result()
	require.NoError(t, err)
	assert.False(t, exists, "meta.paused must be removed after Resume")

	// Resume must add the base marker so blocking workers wake up.
	markerScore, err := rdb.ZScore(ctx, base+"marker", "0").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, markerScore, "marker must hold the base entry after Resume")
}

// TestQueue_Add_DuringPause_GoesToPausedList is the orphan-safety
// acceptance criterion: a job enqueued while the queue is paused must
// land in the paused list (not wait), and Resume must return it to wait.
func TestQueue_Add_DuringPause_GoesToPausedList(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))

	job, err := queue.Add(ctx, testPayload{Inbox: "enqueued-while-paused"})
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	pausedMembers, err := rdb.LRange(ctx, base+"paused", 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{job.ID}, pausedMembers, "job added during pause must go to paused")

	waitLen, err := rdb.LLen(ctx, base+"wait").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, waitLen, "wait must stay empty for a job added during pause")

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, counts.Paused, "Counts.Paused must include the job added during pause")

	require.NoError(t, queue.Resume(ctx))

	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{job.ID}, waitMembers, "job must return to wait after Resume")
}

// TestQueue_PauseResume_Idempotent confirms pause-7.lua's no-op
// semantics: pausing an already-paused queue (or resuming an unpaused
// one) is harmless and never errors.
func TestQueue_PauseResume_Idempotent(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Resume on a never-paused queue is a no-op.
	require.NoError(t, queue.Resume(ctx))
	paused, err := queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.False(t, paused)

	require.NoError(t, queue.Pause(ctx))
	require.NoError(t, queue.Pause(ctx), "double Pause must not error")
	paused, err = queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.True(t, paused)

	require.NoError(t, queue.Resume(ctx))
	require.NoError(t, queue.Resume(ctx), "double Resume must not error")
	paused, err = queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.False(t, paused)
}

// TestQueue_Pause_WorkerDoesNotFetch verifies the end-to-end gate: a job
// parked while paused is not handed to a worker until Resume. The same
// worker that idles through the pause must pick the job up promptly once
// the queue resumes (marker poke).
func TestQueue_Pause_WorkerDoesNotFetch(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))

	job, err := queue.Add(ctx, testPayload{Inbox: "gated"})
	require.NoError(t, err)

	processed := make(chan string, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	// The worker must NOT fetch the parked job while paused.
	select {
	case id := <-processed:
		t.Fatalf("worker processed job %s while queue was paused", id)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, queue.Resume(ctx))

	select {
	case id := <-processed:
		assert.Equal(t, job.ID, id, "worker must process the job after Resume")
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process the job after Resume")
	}
}

// TestQueue_Pause_PrioritizedNotFetched extends the pause gate to the
// prioritized bucket. The moveToActive gate returns before the
// prioritized->active move, so a prioritized job must not be dispatched
// while paused — and it stays in the prioritized ZSET (Pause does not
// rename prioritized into paused), so Counts.Paused excludes it.
func TestQueue_Pause_PrioritizedNotFetched(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))

	job, err := queue.Add(ctx, testPayload{Inbox: "prio-gated"}, mkq.WithPriority(5))
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// Prioritized job lands in the prioritized ZSET, not the paused list.
	prioCard, err := rdb.ZCard(ctx, base+"prioritized").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, prioCard, "prioritized job must stay in the prioritized ZSET")
	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, counts.Prioritized)
	assert.EqualValues(t, 0, counts.Paused, "prioritized job is not counted as paused")

	processed := make(chan string, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	// The prioritized job must NOT be fetched while paused.
	select {
	case id := <-processed:
		t.Fatalf("worker processed prioritized job %s while paused", id)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, queue.Resume(ctx))

	select {
	case id := <-processed:
		assert.Equal(t, job.ID, id, "prioritized job must process after Resume")
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process the prioritized job after Resume")
	}
}

// TestQueue_Pause_DelayedMaturesToPaused is the orphan-safety criterion
// for delayed jobs. A delayed job whose timer fires while the queue is
// paused must be promoted into the paused list (not wait), so it is not
// dispatched until Resume. promoteDelayedJobs runs inside moveToActive
// (driven by the idle worker) and honours the pause target, so the
// matured job is parked, not handed out.
func TestQueue_Pause_DelayedMaturesToPaused(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))

	processed := make(chan string, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	job, err := queue.Add(ctx, testPayload{Inbox: "delayed-gated"}, mkq.WithDelay(200*time.Millisecond))
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// Wait until the idle worker's moveToActive has promoted the matured
	// delayed job out of the delayed ZSET.
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"delayed").Result()
		return n == 0
	})

	// It must have gone to paused, never to wait, and must not be processed.
	pausedMembers, err := rdb.LRange(ctx, base+"paused", 0, -1).Result()
	require.NoError(t, err)
	assert.Contains(t, pausedMembers, job.ID, "matured delayed job must be parked in paused")
	waitLen, err := rdb.LLen(ctx, base+"wait").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, waitLen, "matured delayed job must never leak into wait while paused")

	select {
	case id := <-processed:
		t.Fatalf("worker processed delayed job %s while paused", id)
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, queue.Resume(ctx))

	select {
	case id := <-processed:
		assert.Equal(t, job.ID, id, "delayed job must process after Resume")
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process the delayed job after Resume")
	}
}

// TestQueue_Pause_ClusterHonorAcrossClients verifies the multi-process
// acceptance criterion: a pause issued through one Client is honoured by
// a worker created from a different Client against the same Redis. The
// gate lives in moveToActive's Lua (shared meta.paused), so there is no
// client-local pause state for a second process to miss.
func TestQueue_Pause_ClusterHonorAcrossClients(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	cAdmin := newClient(t, prefix)
	cWorker := newClient(t, prefix)

	queueAdmin := mkq.Define[testPayload](cAdmin, "deliver")
	queueWorker := mkq.Define[testPayload](cWorker, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Pause through the admin Client before the worker Client enqueues.
	require.NoError(t, queueAdmin.Pause(ctx))

	job, err := queueWorker.Add(ctx, testPayload{Inbox: "cluster-gated"})
	require.NoError(t, err)

	processed := make(chan string, 1)
	worker, err := mkq.Process(queueWorker, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	// The worker on cWorker must honour the pause set by cAdmin.
	select {
	case id := <-processed:
		t.Fatalf("worker (cWorker) processed job %s despite pause set by cAdmin", id)
	case <-time.After(500 * time.Millisecond):
	}

	// Resume through the admin Client; the worker Client must pick up.
	require.NoError(t, queueAdmin.Resume(ctx))

	select {
	case id := <-processed:
		assert.Equal(t, job.ID, id, "worker must process after admin Client resumes")
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process after cross-Client Resume")
	}
}

// TestQueue_PauseResume_EmitsEvents verifies the BullMQ QueueEvents wire
// contract: Pause / Resume each XADD a "paused" / "resumed" event onto
// the events stream. mkq has no dedicated event struct for these yet, so
// they surface as RawEvent (forward-compatible), which is what
// bull-board / Misskey QueueEvents consumers read.
func TestQueue_PauseResume_EmitsEvents(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	qe := mkq.NewQueueEvents(queue)
	events, _ := startSubscriber(t, qe)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))
	require.NoError(t, queue.Resume(ctx))

	tctx, tcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tcancel()
	got := drainUntil(t, tctx, events, func(xs []mkq.Event) bool {
		var sawPaused, sawResumed bool
		for _, ev := range xs {
			if raw, ok := ev.(mkq.RawEvent); ok {
				switch raw.Type {
				case "paused":
					sawPaused = true
				case "resumed":
					sawResumed = true
				}
			}
		}
		return sawPaused && sawResumed
	})

	var types []string
	for _, ev := range got {
		if raw, ok := ev.(mkq.RawEvent); ok {
			types = append(types, raw.Type)
		}
	}
	assert.Contains(t, types, "paused", "Pause must XADD a paused event")
	assert.Contains(t, types, "resumed", "Resume must XADD a resumed event")
}
