package mkq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/shiroha-a/mkq/internal/lua"
	"github.com/shiroha-a/mkq/internal/proto"
)

// Handler is the user function invoked once per dequeued job. The
// returned value (any) is JSON-encoded and stored in the BullMQ
// `returnvalue` HASH field on success. A non-nil error transitions the
// job to failed; mkq's first worker PR does not yet retry, regardless
// of WithAttempts.
type Handler[T any] func(ctx context.Context, job *Job[T]) (any, error)

// Worker is the lifecycle handle returned by Process. It owns its
// goroutine pool and the per-job lock heartbeats.
type Worker struct {
	cfg     workerConfig
	keys    queueKeys
	scripts *lua.Scripter

	// runCtx is cancelled by Stop to break dispatch goroutines out of
	// the dequeue loop. Each in-flight handler derives its job ctx
	// from runCtx, so cancellation propagates automatically.
	runCtx    context.Context
	runCancel context.CancelFunc

	// run tracks every dispatch goroutine. Because dispatchLoop runs
	// the handler synchronously (one in-flight job per loop), this
	// also covers handler completion — no separate WaitGroup needed.
	run sync.WaitGroup
}

// queueKeys is a snapshot of the per-queue Redis keys consumed by
// moveToActive / moveToFinished / extendLock / releaseLock. We capture
// it at Process time so the worker can run without holding a *Queue.
type queueKeys struct {
	wait, active, prioritized, events, stalled string
	limiter, delayed, paused, meta, pc, marker string
	completed, failed                          string
	prefix                                     string
}

// Process starts a worker that pulls jobs from q and runs h on each.
// Process returns once the worker goroutines are up; call Worker.Stop
// to drain.
func Process[T any](q *Queue[T], h Handler[T], opts ...WorkerOption) (*Worker, error) {
	if q == nil {
		return nil, errors.New("mkq: Process requires a non-nil queue")
	}
	if h == nil {
		return nil, errors.New("mkq: Process requires a non-nil handler")
	}

	cfg := workerConfig{
		concurrency:      defaultConcurrency,
		lockDuration:     defaultLockDuration,
		idlePollInterval: defaultIdlePollInterval,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}
	if cfg.workerName == "" {
		cfg.workerName = defaultWorkerName()
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		cfg:       cfg,
		keys:      newQueueKeys(q),
		scripts:   q.client.scripts,
		runCtx:    ctx,
		runCancel: cancel,
	}

	shim := newHandlerShim(h)
	for i := 0; i < cfg.concurrency; i++ {
		w.run.Add(1)
		go w.dispatchLoop(shim)
	}
	return w, nil
}

// Stop signals the worker to stop dequeueing and waits for in-flight
// jobs to finish. If ctx is cancelled before the in-flight jobs
// complete, Stop returns ctx.Err(); the still-running handlers see
// their own ctx cancelled and any subsequent moveToFinished call may
// fail with a lock-mismatch error (logged, not surfaced).
func (w *Worker) Stop(ctx context.Context) error {
	w.runCancel()
	loopDone := make(chan struct{})
	go func() {
		w.run.Wait()
		close(loopDone)
	}()
	select {
	case <-loopDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// dispatchLoop is one slot of the goroutine pool. It repeatedly tries
// to dequeue a job and process it, sleeping IdlePollInterval between
// empty pulls and exiting when runCtx is cancelled.
func (w *Worker) dispatchLoop(handlerAny any) {
	defer w.run.Done()
	for {
		if w.runCtx.Err() != nil {
			return
		}
		processed, err := w.tryOnce(handlerAny)
		if err != nil {
			// 取得や lua 失敗はログ的扱い (worker は止めない)。
			// 本格的なエラー報告チャネルは observability PR で導入。
			select {
			case <-time.After(w.cfg.idlePollInterval):
			case <-w.runCtx.Done():
				return
			}
			continue
		}
		if processed {
			continue
		}
		select {
		case <-time.After(w.cfg.idlePollInterval):
		case <-w.runCtx.Done():
			return
		}
	}
}

// tryOnce performs one dequeue attempt and, if successful, runs the
// handler and finalises the job. Returns (processed, err).
func (w *Worker) tryOnce(handlerAny any) (bool, error) {
	token := uuid.NewString()
	now := time.Now().UnixMilli()

	optsBytes, err := proto.EncodeMoveToActiveOpts(proto.MoveToActiveOpts{
		Token:        token,
		LockDuration: w.cfg.lockDuration.Milliseconds(),
		Name:         w.cfg.workerName,
	})
	if err != nil {
		return false, fmt.Errorf("encode moveToActive opts: %w", err)
	}

	res, err := w.scripts.Run(
		w.runCtx,
		lua.MoveToActive,
		w.keys.moveToActiveKeys(),
		w.keys.prefix,
		now,
		optsBytes,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return false, nil
		}
		return false, fmt.Errorf("moveToActive: %w", err)
	}

	jobMap, jobID, ok, err := parseMoveToActiveResult(res)
	if err != nil {
		return false, fmt.Errorf("parse moveToActive: %w", err)
	}
	if !ok {
		return false, nil
	}

	w.processJob(handlerAny, token, jobID, jobMap)
	return true, nil
}

// processJob runs the handler for one acquired job, manages the lock
// heartbeat, and writes the terminal state via the appropriate
// BullMQ Lua script (moveToFinished / retryJob / moveToDelayed).
//
// The per-job ctx is derived from runCtx so worker shutdown propagates
// without a separate bridge goroutine. Heartbeat owns its own goroutine
// because the BullMQ extendLock semantics require periodic ticks
// independent of handler progress.
func (w *Worker) processJob(handlerAny any, token, jobID string, jobMap map[string]string) {
	jobCtx, cancelJob := context.WithCancel(w.runCtx)
	defer cancelJob()

	hbDone := make(chan struct{})
	go w.heartbeat(jobCtx, hbDone, token, jobID, cancelJob)

	outcome := w.runHandler(jobCtx, handlerAny, jobID, jobMap)

	// Heartbeat must finish before any finalisation Lua call so its
	// extendLock won't race with the script's lock-token validation.
	cancelJob()
	<-hbDone

	if err := w.finalise(jobID, token, jobMap, outcome); err != nil {
		// Best-effort lock cleanup so a failed terminal call doesn't
		// strand the lock for the full TTL.
		_, _ = w.scripts.Run(
			context.Background(),
			lua.ReleaseLock,
			[]string{w.keys.jobLock(jobID)},
			token,
			w.cfg.lockDuration.Milliseconds(),
		)
	}
}

// handlerOutcome captures the result of invoking a handler. Exactly
// one of returnValue / errReason is populated. stacktrace mirrors
// BullMQ's HASH `stacktrace` JSON-array shape and is set on every
// failure (panics include the captured Go stack; plain errors carry
// the message itself).
type handlerOutcome struct {
	success     bool
	returnValue string // JSON-encoded user return, success=true only
	err         error  // raw error from the handler, success=false only
	errReason   string // BullMQ failedReason (plain string)
	stacktrace  string // BullMQ stacktrace (JSON array string)
}

// runHandler invokes the user handler under panic recovery and returns
// the BullMQ-shaped wire fields. BullMQ's wire format is asymmetric:
//
//   - returnvalue is JSON.stringify-d (opaque user return).
//   - failedReason is the raw err.message string (no JSON quoting).
//   - stacktrace is JSON.stringify of an array of strings.
//
// Mirroring that asymmetry is required for bull-board and other
// foreign readers to render values without spurious quotes.
func (w *Worker) runHandler(ctx context.Context, handlerAny any, jobID string, jobMap map[string]string) (out handlerOutcome) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			reason := fmt.Sprintf("panic: %v\n%s", r, stack)
			out = handlerOutcome{
				success:    false,
				err:        fmt.Errorf("mkq: handler panic: %v", r),
				errReason:  reason,
				stacktrace: mustJSONString([]string{reason}),
			}
		}
	}()

	ret, err := invokeHandler(ctx, handlerAny, jobID, jobMap)
	if err != nil {
		return handlerOutcome{
			success:    false,
			err:        err,
			errReason:  err.Error(),
			stacktrace: mustJSONString([]string{err.Error()}),
		}
	}
	return handlerOutcome{
		success:     true,
		returnValue: mustJSONString(ret),
	}
}

// heartbeat extends the job's lock at lockDuration/2 intervals until
// the per-job ctx is cancelled or the extend call fails. A failed
// extend cancels the job ctx (cancelJob) so the handler observes a
// cancellation and finishes promptly.
func (w *Worker) heartbeat(ctx context.Context, done chan<- struct{}, token, jobID string, cancelJob context.CancelFunc) {
	defer close(done)
	tick := max(w.cfg.lockDuration/2, time.Second)
	t := time.NewTicker(tick)
	defer t.Stop()
	keys := []string{w.keys.jobLock(jobID), w.keys.stalled}
	lockMs := w.cfg.lockDuration.Milliseconds()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			res, err := w.scripts.Run(context.Background(), lua.ExtendLock, keys, token, lockMs, jobID)
			if err != nil {
				cancelJob()
				return
			}
			if n, ok := res.(int64); ok && n == 0 {
				// Lock no longer ours (stolen via stalled detection or
				// flushed). Cancel the handler so it doesn't keep
				// running off-lock.
				cancelJob()
				return
			}
		}
	}
}

// finalise dispatches the terminal-state Lua call for a job. On
// success it always calls moveToFinished("completed"). On failure it
// consults retry policy and may instead call retryJob (immediate
// re-enqueue) or moveToDelayed (backoff-delayed re-enqueue), bumping
// the BullMQ HASH `atm` (attempts made) counter atomically inside
// the script.
func (w *Worker) finalise(jobID, token string, jobMap map[string]string, out handlerOutcome) error {
	if out.success {
		return w.finishCompleted(jobID, token, out.returnValue)
	}

	// Decide retry. Reading from the in-memory jobMap snapshot is fine
	// for attempts/backoff (immutable), but `atm` lives in Redis and
	// may have been bumped by a prior retry on a different worker —
	// we re-read it (via jobMap, which is the HGETALL captured at
	// moveToActive). For BullMQ-correctness this is sufficient because
	// stalled-recovery — the only way `atm` advances without us
	// observing it — is not yet implemented.
	jobOpts := parseJobOpts(jobMap["opts"])
	atm := parseInt(jobMap["atm"])

	if !w.shouldRetry(jobOpts, atm, out.err) {
		return w.finishFailed(jobID, token, out.errReason, out.stacktrace)
	}

	delay := computeBackoffDelay(jobOpts.backoff, atm+1)
	if delay > 0 {
		return w.retryDelayed(jobID, token, delay, out.errReason, out.stacktrace)
	}
	return w.retryImmediate(jobID, token, jobOpts.lifo, out.errReason, out.stacktrace)
}

// finishCompleted writes the BullMQ completed-state transition.
func (w *Worker) finishCompleted(jobID, token, returnValue string) error {
	return w.runMoveToFinished(jobID, token, "completed", "returnvalue", returnValue, nil)
}

// finishFailed writes the BullMQ failed-state transition with the
// failedReason + stacktrace HASH fields.
func (w *Worker) finishFailed(jobID, token, reason, stacktrace string) error {
	return w.runMoveToFinished(jobID, token, "failed", "failedReason", reason,
		[]any{"stacktrace", stacktrace})
}

// runMoveToFinished is the shared moveToFinished invocation. extraFields
// piggybacks on ARGV[9] for the failed path to write stacktrace
// alongside the state change.
func (w *Worker) runMoveToFinished(jobID, token, target, msgProperty, msgValue string, extraFields []any) error {
	now := time.Now().UnixMilli()

	optsBytes, err := proto.EncodeMoveToFinishedOpts(proto.MoveToFinishedOpts{
		Token:        token,
		LockDuration: w.cfg.lockDuration.Milliseconds(),
		Name:         w.cfg.workerName,
	})
	if err != nil {
		return fmt.Errorf("encode finish opts: %w", err)
	}

	fields := []any{"finishedOn", now}
	fields = append(fields, extraFields...)
	jobFields, err := proto.EncodeJobFields(fields...)
	if err != nil {
		return fmt.Errorf("encode job fields: %w", err)
	}

	keys := w.keys.moveToFinishedKeys(jobID, target)
	res, err := w.scripts.Run(
		context.Background(),
		lua.MoveToFinished,
		keys,
		jobID,
		now,
		msgProperty,
		msgValue,
		target,
		"", // fetchNext=false: empty string is BullMQ's "do not fetch" sentinel
		w.keys.prefix,
		optsBytes,
		jobFields,
	)
	if err != nil {
		return fmt.Errorf("moveToFinished(%s): %w", target, err)
	}
	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("moveToFinished(%s) returned error code %d", target, code)
	}
	return nil
}

// retryImmediate re-enqueues a failed job via retryJob-11.lua. The Lua
// bumps `atm` (HINCRBY) atomically and writes failedReason/stacktrace
// via ARGV[6] (jobFieldsToUpdate, msgpack flat array).
func (w *Worker) retryImmediate(jobID, token string, lifo bool, reason, stacktrace string) error {
	now := time.Now().UnixMilli()
	pushCmd := "LPUSH"
	if lifo {
		pushCmd = "RPUSH"
	}

	jobFields, err := proto.EncodeJobFields("failedReason", reason, "stacktrace", stacktrace)
	if err != nil {
		return fmt.Errorf("encode retry job fields: %w", err)
	}

	keys := w.keys.retryJobKeys(jobID)
	res, err := w.scripts.Run(
		context.Background(),
		lua.RetryJob,
		keys,
		w.keys.prefix,
		now,
		pushCmd,
		jobID,
		token,
		jobFields,
	)
	if err != nil {
		return fmt.Errorf("retryJob: %w", err)
	}
	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("retryJob returned error code %d", code)
	}
	return nil
}

// retryDelayed re-enqueues with a delay via moveToDelayed-12.lua,
// honouring exponential / fixed backoff computed Go-side.
func (w *Worker) retryDelayed(jobID, token string, delay time.Duration, reason, stacktrace string) error {
	now := time.Now().UnixMilli()
	// 防御: computeBackoffDelay は >0 のはずだが、誤って 0 を渡すと
	// Lua が delayedTimestamp = timestamp + 0 で即時 promote 候補に
	// なる。1 ms 床値で「delayed」状態を確実にする。
	delayMs := max(delay.Milliseconds(), 1)

	jobFields, err := proto.EncodeJobFields("failedReason", reason, "stacktrace", stacktrace)
	if err != nil {
		return fmt.Errorf("encode retry job fields: %w", err)
	}

	keys := w.keys.moveToDelayedKeys(jobID)
	res, err := w.scripts.Run(
		context.Background(),
		lua.MoveToDelayed,
		keys,
		w.keys.prefix,
		now,
		jobID,
		token,
		delayMs,
		"0", // skip attempt = false: bump atm on this retry
		jobFields,
		"", // fetchNext=false
		"", // opts: BullMQ accepts empty for the basic retry case
	)
	if err != nil {
		return fmt.Errorf("moveToDelayed: %w", err)
	}
	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("moveToDelayed returned error code %d", code)
	}
	return nil
}

// jobOpts is the subset of the per-job opts JSON that the worker
// needs after dequeue. The on-Redis HASH `opts` field is the JSON
// shape produced by Job.optsAsJSON in BullMQ TS.
type jobOpts struct {
	attempts int
	backoff  *BackoffStrategy
	lifo     bool
}

// parseJobOpts deserialises the BullMQ HASH `opts` JSON into the
// fields the retry path needs. Missing or malformed input falls back
// to zero values (no retry / no backoff / FIFO push).
func parseJobOpts(raw string) jobOpts {
	var out jobOpts
	if raw == "" || raw == "{}" {
		return out
	}
	var m struct {
		Attempts int  `json:"attempts"`
		Lifo     bool `json:"lifo"`
		Backoff  *struct {
			Type  string `json:"type"`
			Delay int64  `json:"delay"`
		} `json:"backoff"`
	}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return out
	}
	out.attempts = m.Attempts
	out.lifo = m.Lifo
	if m.Backoff != nil {
		out.backoff = &BackoffStrategy{
			Type:  m.Backoff.Type,
			Delay: time.Duration(m.Backoff.Delay) * time.Millisecond,
		}
	}
	return out
}

// shouldRetry encodes BullMQ's worker-side retry decision. attemptsMade
// is the count BEFORE this failure (the value stored in HASH `atm`);
// the +1 mirrors BullMQ's `attemptsMade + 1 < opts.attempts` check.
func (w *Worker) shouldRetry(o jobOpts, attemptsMade int, err error) bool {
	if errors.Is(err, ErrUnrecoverable) {
		return false
	}
	if o.attempts <= 0 {
		return false
	}
	return attemptsMade+1 < o.attempts
}

func parseInt(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// invokeHandler dispatches to the typed Handler[T] via the closure
// captured at Process time. We pay one type assertion per job to keep
// Worker / Queue independent of T.
func invokeHandler(ctx context.Context, handlerAny any, jobID string, jobMap map[string]string) (any, error) {
	invoke, ok := handlerAny.(func(ctx context.Context, jobID string, jobMap map[string]string) (any, error))
	if !ok {
		return nil, fmt.Errorf("mkq: handler shim has unexpected type %T", handlerAny)
	}
	return invoke(ctx, jobID, jobMap)
}

// newHandlerShim wraps a typed Handler[T] in the type-erased adapter
// the Worker invokes. Lives here so worker.go owns the only place that
// reaches into Job[T] reconstruction.
func newHandlerShim[T any](h Handler[T]) any {
	return func(ctx context.Context, jobID string, jobMap map[string]string) (any, error) {
		job, err := buildJob[T](jobID, jobMap)
		if err != nil {
			return nil, fmt.Errorf("mkq: rebuild job %s: %w", jobID, err)
		}
		return h(ctx, job)
	}
}

func buildJob[T any](jobID string, jobMap map[string]string) (*Job[T], error) {
	var data T
	if raw, ok := jobMap["data"]; ok && raw != "" {
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			return nil, fmt.Errorf("unmarshal data: %w", err)
		}
	}
	tsMs, _ := strconv.ParseInt(jobMap["timestamp"], 10, 64)
	return &Job[T]{
		ID:        jobID,
		Name:      jobMap["name"],
		Data:      data,
		Timestamp: time.UnixMilli(tsMs),
	}, nil
}

// parseMoveToActiveResult interprets the polymorphic EVALSHA response.
// Lua returns one of:
//   - {HGETALL_array, jobId, 0, 0} on success
//   - {0, 0, expireTime, 0} when rate-limited (treated as no job)
//   - {0, 0, 0, nextDelayedTs} when waiting on a delayed job
//   - {0, 0, 0, 0} when paused / maxed / empty
func parseMoveToActiveResult(res any) (jobMap map[string]string, jobID string, ok bool, err error) {
	arr, isArr := res.([]any)
	if !isArr || len(arr) < 2 {
		return nil, "", false, nil
	}
	jobData, isJobData := arr[0].([]any)
	if !isJobData {
		// arr[0] が integer (=0) のとき = job 無し。
		return nil, "", false, nil
	}
	id, idOK := arr[1].(string)
	if !idOK || id == "" {
		return nil, "", false, nil
	}
	m, err := flatHashToMap(jobData)
	if err != nil {
		return nil, "", false, err
	}
	return m, id, true, nil
}

// flatHashToMap turns Redis HGETALL output `[k1, v1, k2, v2, ...]` into
// a map. go-redis surfaces strings here.
func flatHashToMap(arr []any) (map[string]string, error) {
	if len(arr)%2 != 0 {
		return nil, fmt.Errorf("HGETALL flat array has odd length %d", len(arr))
	}
	m := make(map[string]string, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		k, kok := arr[i].(string)
		v, vok := arr[i+1].(string)
		if !kok || !vok {
			return nil, fmt.Errorf("HGETALL element %d/%d not a string", i, i+1)
		}
		m[k] = v
	}
	return m, nil
}

// mustJSONString JSON-encodes v; on failure (which would only happen
// for genuinely non-marshalable values) it falls back to a string so
// the failed/completed transition still surfaces *something*.
func mustJSONString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		fallback, _ := json.Marshal(fmt.Sprintf("mkq: failed to marshal value: %v", err))
		return string(fallback)
	}
	return string(b)
}

// defaultWorkerName matches BullMQ's "host:pid:uuid" convention loosely.
func defaultWorkerName() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8])
}

// newQueueKeys snapshots the per-queue Redis key set used by all four
// worker scripts.
func newQueueKeys[T any](q *Queue[T]) queueKeys {
	b := q.keys
	return queueKeys{
		wait:        b.Wait(),
		active:      b.Active(),
		prioritized: b.Prioritized(),
		events:      b.Events(),
		stalled:     b.Stalled(),
		limiter:     b.Limiter(),
		delayed:     b.Delayed(),
		paused:      b.Paused(),
		meta:        b.Meta(),
		pc:          b.PriorityCounter(),
		marker:      b.Marker(),
		completed:   b.Completed(),
		failed:      b.Failed(),
		prefix:      b.Base(),
	}
}

func (k queueKeys) jobLock(jobID string) string {
	return k.prefix + jobID + ":lock"
}

func (k queueKeys) job(jobID string) string {
	return k.prefix + jobID
}

// moveToActiveKeys assembles KEYS[1..11] for moveToActive-11.lua.
func (k queueKeys) moveToActiveKeys() []string {
	return []string{
		k.wait, k.active, k.prioritized, k.events, k.stalled,
		k.limiter, k.delayed, k.paused, k.meta, k.pc,
		k.marker,
	}
}

// moveToFinishedKeys assembles KEYS[1..14] for moveToFinished-14.lua.
// KEYS[11] toggles between completed and failed depending on target;
// KEYS[13] (metrics key) is intentionally empty until observability
// lands.
func (k queueKeys) moveToFinishedKeys(jobID, target string) []string {
	resultSet := k.completed
	if target == "failed" {
		resultSet = k.failed
	}
	return []string{
		k.wait, k.active, k.prioritized, k.events, k.stalled,
		k.limiter, k.delayed, k.paused, k.meta, k.pc,
		resultSet, k.job(jobID), "", k.marker,
	}
}

// retryJobKeys assembles KEYS[1..11] for retryJob-11.lua.
//
//	KEYS[1]  active     KEYS[2]  wait        KEYS[3]  paused
//	KEYS[4]  job key    KEYS[5]  meta        KEYS[6]  events
//	KEYS[7]  delayed    KEYS[8]  prioritized KEYS[9]  pc
//	KEYS[10] marker     KEYS[11] stalled
func (k queueKeys) retryJobKeys(jobID string) []string {
	return []string{
		k.active, k.wait, k.paused, k.job(jobID), k.meta, k.events,
		k.delayed, k.prioritized, k.pc, k.marker, k.stalled,
	}
}

// moveToDelayedKeys assembles KEYS[1..12] for moveToDelayed-12.lua.
//
//	KEYS[1]  marker     KEYS[2]  active      KEYS[3]  prioritized
//	KEYS[4]  delayed    KEYS[5]  job key     KEYS[6]  events
//	KEYS[7]  meta       KEYS[8]  stalled     KEYS[9]  wait
//	KEYS[10] limiter    KEYS[11] paused      KEYS[12] pc
func (k queueKeys) moveToDelayedKeys(jobID string) []string {
	return []string{
		k.marker, k.active, k.prioritized, k.delayed, k.job(jobID),
		k.events, k.meta, k.stalled, k.wait, k.limiter,
		k.paused, k.pc,
	}
}
