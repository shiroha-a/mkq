package mkq

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/shiroha-a/mkq/internal/lua"
)

// Config configures a Client.
type Config struct {
	// Redis is forwarded to redis.NewUniversalClient. Addrs at minimum
	// must be set.
	Redis redis.UniversalOptions
	// KeyPrefix maps to BullMQ's "keyPrefix" option. Empty defaults to
	// "bull" to match BullMQ's own default.
	KeyPrefix string

	// Logger receives operational records (stalled-scan failures,
	// NOSCRIPT reload events, etc.). Nil = noop. Adapters live under
	// github.com/shiroha-a/mkq/observability/... — opt-in.
	Logger Logger
	// Metrics receives counter / histogram / gauge updates from the
	// hot paths. Nil = noop. See the Metrics interface godoc for the
	// signal set mkq emits.
	Metrics Metrics
	// Tracer creates spans around Queue.Add and handler invocation.
	// Nil = noop. See the Tracer interface godoc for span names.
	Tracer Tracer
}

// Client is a long-lived handle to a Redis-backed mkq deployment. A
// Client owns the underlying redis.UniversalClient and the cached Lua
// SHAs; queues constructed via Define share both.
type Client struct {
	rdb       redis.UniversalClient
	keyPrefix string
	scripts   *lua.Scripter
	logger    Logger
	metrics   Metrics
	tracer    Tracer
	// configuredPoolSize は cfg.Redis.PoolSize の保存。0 は go-redis の
	// 既定値 (10*GOMAXPROCS) を意味する。Worker 起動時の pool-size
	// 警告に使う。
	configuredPoolSize int
}

// NewClient connects to Redis, preloads the vendored BullMQ Lua scripts,
// and returns a ready-to-use Client. Callers must Close the returned
// Client to release the Redis connection pool.
//
// The context bounds connection setup (PING + SCRIPT LOAD); cancel it
// to abort a hung connect attempt.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Redis.Addrs) == 0 {
		return nil, errors.New("mkq: Config.Redis.Addrs is required")
	}
	rdb := redis.NewUniversalClient(&cfg.Redis)

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("mkq: redis ping: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = noopLogger{}
	}
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = noopMetrics{}
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = noopTracer{}
	}

	scripts, err := lua.NewScripter(ctx, rdb, logger)
	if err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("mkq: load scripts: %w", err)
	}

	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = defaultKeyPrefix
	}

	return &Client{
		rdb:                rdb,
		keyPrefix:          prefix,
		configuredPoolSize: cfg.Redis.PoolSize,
		scripts:            scripts,
		logger:             logger,
		metrics:            metrics,
		tracer:             tracer,
	}, nil
}

// Close releases the underlying Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// defaultKeyPrefix matches BullMQ's default. Mirrored here rather than
// imported from internal/keys to keep the public surface free of
// internal-package leaks.
const defaultKeyPrefix = "bull"

// queueRegistryKey is the auxiliary SET that tracks every queue
// Define[T] has touched against this Client. The key uses a double
// colon (empty queue-name slot) so it cannot collide with any
// BullMQ queue key — BullMQ requires a non-empty queue name, so
// `{prefix}::queues` is unreachable from the BullMQ wire format.
//
// BullMQ itself has no queue registry (bull-board accepts the queue
// list as configuration); this is an mkq-only addition. Foreign
// queues that BullMQ TS created without mkq Define'ing them won't
// appear here — the registry under-reports rather than over-reports.
func (c *Client) queueRegistryKey() string {
	return c.keyPrefix + "::queues"
}

// Queues returns every queue name mkq has Define'd against this
// client (idempotent registration in Define keeps the SET clean).
//
// Foreign queues not Define'd through mkq are absent; for a complete
// view, mk-go-style admin paths can layer a SCAN-based discovery on
// top — that's a separate API addition tracked in the inspector
// follow-up.
func (c *Client) Queues(ctx context.Context) ([]string, error) {
	res, err := c.rdb.SMembers(ctx, c.queueRegistryKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("mkq: SMembers %s: %w", c.queueRegistryKey(), err)
	}
	return res, nil
}
