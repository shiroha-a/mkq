package mkq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/shiroha-a/mkq/internal/lua"
)

// UpdateProgress reports a progress value for the job. The value is
// JSON-encoded and stored in the BullMQ `progress` HASH field; a
// `progress` event is also XADDed so QueueEvents subscribers see the
// update in real time.
//
// Mirrors BullMQ's Job.updateProgress: best-effort, no lock-token
// validation. A worker that has lost its lock to stalled-recovery
// can race with the new owner; mkq does not guard against that
// (BullMQ TS doesn't either — the contract is "use it from your own
// handler").
//
// Returns ErrJobNotFound when the underlying HASH no longer exists
// (e.g. the job was finalised with WithKeepCompleted(0)) and
// ErrJobDetached when the Job was constructed outside the mkq API.
func (j *Job[T]) UpdateProgress(ctx context.Context, progress any) error {
	if j.queue == nil {
		return ErrJobDetached
	}
	return j.queue.UpdateJobProgress(ctx, j.ID, progress)
}

// UpdateData replaces the job's `data` HASH field with the JSON
// encoding of d. Useful for handlers that need to persist
// intermediate computation across retries — the next attempt will
// see the new data via buildJob.
//
// Same best-effort / lock-token caveats as UpdateProgress.
func (j *Job[T]) UpdateData(ctx context.Context, d T) error {
	if j.queue == nil {
		return ErrJobDetached
	}
	return j.queue.UpdateJobData(ctx, j.ID, d)
}

// Log appends a single log line to the per-job logs LIST
// (`{prefix}:{queue}:{jobID}:logs`). bull-board surfaces this list
// in its log tab. mkq does not currently apply the BullMQ `keepLogs`
// trim — every Log() call grows the list. A WithKeepLogs option can
// land in a follow-up if retention becomes a concern.
//
// Same best-effort / lock-token caveats as UpdateProgress.
func (j *Job[T]) Log(ctx context.Context, line string) error {
	if j.queue == nil {
		return ErrJobDetached
	}
	return j.queue.AppendJobLog(ctx, j.ID, line)
}

// UpdateJobProgress is the out-of-band counterpart to
// Job.UpdateProgress: callers who hold a Queue handle and a jobID
// (e.g. an admin reporter goroutine) can update progress without
// dequeuing the job.
//
// progress is JSON-encoded; pass any value the user-defined
// JobState.Progress json.RawMessage consumer expects (number,
// string, object).
func (q *Queue[T]) UpdateJobProgress(ctx context.Context, jobID string, progress any) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	progJSON, err := json.Marshal(progress)
	if err != nil {
		return fmt.Errorf("mkq: marshal progress: %w", err)
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.UpdateProgress,
		[]string{q.keys.Job(jobID), q.keys.Events(), q.keys.Meta()},
		jobID,
		string(progJSON),
	)
	if err != nil {
		return fmt.Errorf("mkq: updateProgress: %w", err)
	}
	return classifyMutationResult(res)
}

// UpdateJobData is the out-of-band counterpart to Job.UpdateData.
func (q *Queue[T]) UpdateJobData(ctx context.Context, jobID string, data T) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("mkq: marshal data: %w", err)
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.UpdateData,
		[]string{q.keys.Job(jobID)},
		string(dataJSON),
	)
	if err != nil {
		return fmt.Errorf("mkq: updateData: %w", err)
	}
	return classifyMutationResult(res)
}

// AppendJobLog is the out-of-band counterpart to Job.Log.
func (q *Queue[T]) AppendJobLog(ctx context.Context, jobID, line string) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.AddLog,
		[]string{q.keys.Job(jobID), q.keys.JobLogs(jobID)},
		jobID,
		line,
		"", // keepLogs="" → keep all (BullMQ default when retention not configured)
	)
	if err != nil {
		return fmt.Errorf("mkq: addLog: %w", err)
	}
	// addLog returns the resulting list length on success and -1 on
	// missing-job. Any non-negative integer counts as success.
	if code, ok := res.(int64); ok && code < 0 {
		return ErrJobNotFound
	}
	return nil
}

// classifyMutationResult maps the shared 0/-1 return of updateProgress
// and updateData to nil / ErrJobNotFound. Lua scripts that use a
// different sentinel (addLog returns the new list length) call sites
// inline their own check.
func classifyMutationResult(res any) error {
	code, ok := res.(int64)
	if !ok {
		return nil
	}
	if code == -1 {
		return ErrJobNotFound
	}
	if code < 0 {
		return fmt.Errorf("mkq: mutation lua returned error code %d", code)
	}
	return nil
}
