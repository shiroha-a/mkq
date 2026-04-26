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
}

// Client is a long-lived handle to a Redis-backed mkq deployment. A
// Client owns the underlying redis.UniversalClient and the cached Lua
// SHAs; queues constructed via Define share both.
type Client struct {
	rdb       redis.UniversalClient
	keyPrefix string
	scripts   *lua.Scripter
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

	scripts, err := lua.NewScripter(ctx, rdb)
	if err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("mkq: load scripts: %w", err)
	}

	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = defaultKeyPrefix
	}

	return &Client{
		rdb:       rdb,
		keyPrefix: prefix,
		scripts:   scripts,
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
