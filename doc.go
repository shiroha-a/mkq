// Package mkq is a BullMQ-compatible Go-native job queue.
//
// mkq stores jobs in Redis using the same key layout, JSON shape, and
// Lua atomic semantics as BullMQ v5+, so foreign workers in any
// language—or admin tools like bull-board—can share queues with mkq
// without translation.
//
// The public API is generic over the job payload type:
//
//	type DeliverPayload struct {
//	    Inbox string `json:"inbox"`
//	    Body  []byte `json:"body"`
//	}
//
//	client, err := mkq.NewClient(ctx, mkq.Config{
//	    Redis: redis.UniversalOptions{Addrs: []string{"localhost:6379"}},
//	})
//	if err != nil { ... }
//	defer client.Close()
//
//	queue := mkq.Define[DeliverPayload](client, "deliver")
//	job, err := queue.Add(ctx, DeliverPayload{Inbox: "...", Body: body},
//	    mkq.WithDelay(5*time.Second),
//	    mkq.WithAttempts(8),
//	    mkq.WithBackoff(mkq.ExponentialBackoff(time.Second)),
//	)
//
// See https://docs.bullmq.io/ for protocol-level documentation; mkq's
// API surface is intentionally Go-native and does not mirror BullMQ's
// JavaScript class hierarchy.
package mkq
