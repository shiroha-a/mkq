package lua

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// ScriptName identifies a vendored entry-point script.
type ScriptName string

// Vendored entry-point scripts. The numeric suffix mirrors BullMQ's
// convention of encoding the expected KEYS count in the filename.
const (
	AddStandardJob    ScriptName = "addStandardJob-9"
	AddDelayedJob     ScriptName = "addDelayedJob-6"
	AddPrioritizedJob ScriptName = "addPrioritizedJob-9"
	MoveToActive      ScriptName = "moveToActive-11"
	MoveToFinished    ScriptName = "moveToFinished-14"
	ExtendLock        ScriptName = "extendLock-2"
	ReleaseLock       ScriptName = "releaseLock-1"
	RetryJob          ScriptName = "retryJob-11"
	MoveToDelayed     ScriptName = "moveToDelayed-12"
)

// Scripter executes vendored Lua scripts via EVALSHA, with transparent
// re-loading on NOSCRIPT.
//
// A Scripter is safe for concurrent use.
type Scripter struct {
	client redis.Scripter

	mu      sync.RWMutex
	scripts map[ScriptName]*loadedScript
}

type loadedScript struct {
	source string
	sha    string
}

// NewScripter resolves and pre-loads the vendored entry-point scripts
// against the given Redis client. The returned Scripter caches each
// script's SHA so subsequent calls round-trip a single EVALSHA.
func NewScripter(ctx context.Context, client redis.Scripter) (*Scripter, error) {
	s := &Scripter{
		client:  client,
		scripts: make(map[ScriptName]*loadedScript),
	}
	for _, name := range []ScriptName{
		AddStandardJob, AddDelayedJob, AddPrioritizedJob,
		MoveToActive, MoveToFinished, ExtendLock, ReleaseLock,
		RetryJob, MoveToDelayed,
	} {
		if _, err := s.load(ctx, name); err != nil {
			return nil, fmt.Errorf("preload %s: %w", name, err)
		}
	}
	return s, nil
}

// Run executes the named script. On NOSCRIPT (e.g. after a Redis
// SCRIPT FLUSH or restart) the script is re-uploaded and the call is
// retried exactly once; further failures are returned to the caller.
func (s *Scripter) Run(ctx context.Context, name ScriptName, keys []string, args ...any) (any, error) {
	ls, err := s.get(name)
	if err != nil {
		return nil, err
	}
	res, err := s.client.EvalSha(ctx, ls.sha, keys, args...).Result()
	if err == nil {
		return res, nil
	}
	if !isNoScript(err) {
		return nil, err
	}
	// NOSCRIPT: Redisがflushされた等。再ロードして1度だけrety。
	ls, err = s.load(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("reload %s after NOSCRIPT: %w", name, err)
	}
	return s.client.EvalSha(ctx, ls.sha, keys, args...).Result()
}

func (s *Scripter) get(name ScriptName) (*loadedScript, error) {
	s.mu.RLock()
	ls, ok := s.scripts[name]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("script %q not loaded", name)
	}
	return ls, nil
}

func (s *Scripter) load(ctx context.Context, name ScriptName) (*loadedScript, error) {
	src, err := Preprocess(string(name))
	if err != nil {
		return nil, err
	}
	sha, err := s.client.ScriptLoad(ctx, src).Result()
	if err != nil {
		return nil, fmt.Errorf("script load %s: %w", name, err)
	}
	ls := &loadedScript{source: src, sha: sha}
	s.mu.Lock()
	s.scripts[name] = ls
	s.mu.Unlock()
	return ls, nil
}

func isNoScript(err error) bool {
	if err == nil {
		return false
	}
	// go-redis v9 surfaces server-side errors via the redis.Error
	// interface; the string form preserves the original "NOSCRIPT ..."
	// prefix from Redis.
	var rerr redis.Error
	if errors.As(err, &rerr) {
		return strings.HasPrefix(rerr.Error(), "NOSCRIPT")
	}
	return strings.Contains(err.Error(), "NOSCRIPT")
}
