package mkq

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/shiroha-a/mkq/internal/lua"
)

// ListedJob bundles a typed Job with its post-processing JobState
// snapshot — the ListJobs caller usually wants both halves in a
// single round-trip.
type ListedJob[T any] struct {
	Job   *Job[T]
	State *JobState
}

// ListJobs returns the jobs sitting in `bucket` between [start, end]
// (inclusive, zero-based) at the moment of the call. The semantics
// mirror BullMQ's Queue.getJobs: pass `start=0, end=-1` to fetch the
// whole bucket, `start=0, end=N` for the first N+1 entries, and so
// on. asynq-style page/pageSize callers can shim with
// `(start = page*pageSize, end = start+pageSize-1)`.
//
// The bucket determines fetch semantics inside the lua:
//
//   - LIST-backed buckets (wait, paused, active) honour insertion
//     order; ascending=false reads in LIST-native order.
//   - ZSET-backed buckets (delayed, prioritized, completed, failed)
//     read by score. Ascending applies in the obvious sense.
//
// Pass ascending=true to get the oldest-first / lowest-score-first
// view that admin UIs typically want; ascending=false returns the
// BullMQ default (newest first / highest score first).
//
// Each returned ListedJob carries both the typed Job (with the user
// payload decoded into T) and the JobState snapshot (terminal-state
// fields like FinishedOn / ReturnValue). For jobs still in flight
// JobState is non-nil but its time fields stay zero.
func (q *Queue[T]) ListJobs(ctx context.Context, bucket JobBucket, start, end int64, ascending bool) ([]ListedJob[T], error) {
	asc := "0"
	if ascending {
		asc = "1"
	}

	res, err := q.client.scripts.Run(
		ctx,
		lua.GetRanges,
		[]string{q.keys.Base()},
		fmt.Sprintf("%d", start),
		fmt.Sprintf("%d", end),
		asc,
		string(bucket),
	)
	if err != nil {
		return nil, fmt.Errorf("mkq: getRanges: %w", err)
	}

	// getRanges returns one entry per requested bucket; we only ever
	// pass a single bucket, so unwrap the outer wrapper.
	outer, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("mkq: getRanges: unexpected result type %T", res)
	}
	if len(outer) == 0 {
		return nil, nil
	}
	ids, err := toStringSlice(outer[0])
	if err != nil {
		return nil, fmt.Errorf("mkq: getRanges: %w", err)
	}
	if len(ids) == 0 {
		return []ListedJob[T]{}, nil
	}

	return q.fetchJobs(ctx, ids)
}

// fetchJobs HGETALLs every id in a pipeline (one round-trip per
// pipeline batch). Missing HASHes (e.g. job removed between getRanges
// and HGETALL) are skipped silently — the lua's snapshot is point-in-
// time and a follow-up reader can race with a delete.
func (q *Queue[T]) fetchJobs(ctx context.Context, ids []string) ([]ListedJob[T], error) {
	pipe := q.client.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, q.keys.Job(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("mkq: pipeline HGetAll: %w", err)
	}

	out := make([]ListedJob[T], 0, len(ids))
	for i, cmd := range cmds {
		hash, err := cmd.Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return nil, fmt.Errorf("mkq: HGetAll %s: %w", ids[i], err)
		}
		if len(hash) == 0 {
			continue
		}
		job, err := buildJob[T](ids[i], hash)
		if err != nil {
			return nil, fmt.Errorf("mkq: buildJob %s: %w", ids[i], err)
		}
		job.queue = q
		out = append(out, ListedJob[T]{Job: job, State: buildJobState(hash)})
	}
	return out, nil
}

// toStringSlice normalises go-redis's polymorphic Lua-array result
// (often []any of strings) into []string. Empty / nil inputs return
// nil so callers can range over the result safely.
func toStringSlice(v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected []any, got %T", v)
	}
	out := make([]string, 0, len(arr))
	for i, raw := range arr {
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("element %d not a string (%T)", i, raw)
		}
		out = append(out, s)
	}
	return out, nil
}
