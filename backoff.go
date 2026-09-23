package mkq

import (
	"math"
	"time"
)

// BackoffStrategy describes a BullMQ-compatible backoff. The on-Redis
// JSON shape is {"type": "...", "delay": <ms>, "jitter": <0..1>};
// mkq mirrors it verbatim.
type BackoffStrategy struct {
	// Type is "fixed", "exponential" (the values BullMQ recognises out
	// of the box), or "custom" (BullMQ's settings.backoffStrategy
	// path). For "custom", register the computation on the Worker via
	// WithBackoffStrategyFunc (job context available) or the older
	// WithBackoffStrategy (attempt count only).
	Type string
	// Delay is the base delay; Type controls how it scales between
	// retries. Ignored for "custom".
	Delay time.Duration
	// Jitter is the BullMQ jitter fraction (0..1) applied to fixed /
	// exponential delays to spread retries and ease thundering herds.
	// 0 disables jitter. Ignored for "custom" (the registered strategy
	// owns any jitter it wants).
	Jitter float64
}

// CustomBackoffFunc computes the retry delay for a job whose backoff
// Type is not a BullMQ built-in (e.g. "custom"). attemptsMade is the
// post-bump attempt count (i.e. "this is attempt N"), matching the
// argument BullMQ passes to settings.backoffStrategy.
//
// This is where a caller reproduces an arbitrary formula plus cap plus
// jitter Go-side. mkq never persists a cap to Redis (BullMQ has no cap
// field, so a foreign worker would ignore it); capping belongs here.
//
// Returning a negative duration stops the retries and fails the job now,
// matching what BullMQ does when settings.backoffStrategy returns -1.
//
// Implementations must be safe for concurrent use: one worker runs
// `concurrency` dispatch goroutines and each calls the strategy
// independently.
//
// BullMQ's own settings.backoffStrategy also receives the backoff type,
// the handler error and the job; BackoffFunc plus WithBackoffStrategyFunc
// expose those. Prefer them for anything that has to look at more than
// the attempt count.
type CustomBackoffFunc func(attemptsMade int) time.Duration

// BackoffContext carries the job context a custom backoff strategy can
// decide on: what BullMQ hands to settings.backoffStrategy as
// (attemptsMade, type, err), plus the job's id and name.
//
// BullMQ passes the whole job, so a TypeScript strategy that keys its
// delay off the payload has no direct equivalent here — put what the
// delay depends on in the error instead, which is where the decision
// usually lives anyway. Fields can be added later; they cannot be taken
// away.
type BackoffContext struct {
	// JobID is the BullMQ job id.
	JobID string
	// Name is the BullMQ job name, which is how one queue fans out
	// across task types. A strategy can back off differently per type.
	Name string
	// AttemptsMade is the post-bump attempt count (i.e. "this is
	// attempt N"), the same value CustomBackoffFunc receives.
	AttemptsMade int
	// Err is the error the handler returned, or the synthesised error
	// for a panic. Pull a retry hint out of a typed error with
	// errors.As — an HTTP 429's Retry-After, say.
	Err error
	// BackoffType is the job's opts backoff type, "custom" for the
	// BullMQ settings.backoffStrategy path.
	BackoffType string
}

// BackoffFunc computes the retry delay for a job whose backoff Type is
// not a BullMQ built-in, with the full job context available.
//
// **試行回数だけでは決められない遅延がある。** 相手が Retry-After で
// 「30 秒後に来い」と言っているのに固定の式しか持てない、という状況が
// 実際に起きる。CustomBackoffFunc はその文脈を受け取れないので、
// こちらを使う。
//
// Returning a negative duration stops the retries and fails the job now,
// matching what BullMQ does when settings.backoffStrategy returns -1.
//
// Implementations must be safe for concurrent use: one worker runs
// `concurrency` dispatch goroutines and each calls the strategy
// independently. **単一スレッドの BullMQ から移してくると、ホストごとの
// 次回許可時刻を map に溜めるような実装を素直に書いてしまう。** Go では
// そのままだと concurrent map writes で落ちる。
type BackoffFunc func(BackoffContext) time.Duration

// FixedBackoff retries on the same delay every attempt.
func FixedBackoff(d time.Duration) BackoffStrategy {
	return BackoffStrategy{Type: "fixed", Delay: d}
}

// FixedBackoffWithJitter is FixedBackoff with a BullMQ jitter fraction
// (0..1). The effective delay lands in [d*(1-jitter), d).
func FixedBackoffWithJitter(d time.Duration, jitter float64) BackoffStrategy {
	return BackoffStrategy{Type: "fixed", Delay: d, Jitter: jitter}
}

// ExponentialBackoff doubles the delay each attempt (delay * 2^(n-1)).
// BullMQ's built-in exponential strategy does not accept a cap; callers
// that want a ceiling should use a custom strategy (see CustomBackoff /
// WithBackoffStrategy).
func ExponentialBackoff(d time.Duration) BackoffStrategy {
	return BackoffStrategy{Type: "exponential", Delay: d}
}

// ExponentialBackoffWithJitter is ExponentialBackoff with a BullMQ
// jitter fraction (0..1). For attempt n the un-jittered delay is
// delay*2^(n-1); the jittered result lands in [delay*2^(n-1)*(1-jitter),
// delay*2^(n-1)).
func ExponentialBackoffWithJitter(d time.Duration, jitter float64) BackoffStrategy {
	return BackoffStrategy{Type: "exponential", Delay: d, Jitter: jitter}
}

// CustomBackoff marks a job as using the Worker-registered backoff
// strategy (BullMQ's settings.backoffStrategy path). The on-Redis opts
// store backoff as {"type": "custom"}; the actual delay is computed on
// the Worker that processes the job, by the strategy registered via
// WithBackoffStrategyFunc (which sees the job id, name, attempt count,
// error and backoff type) or the older WithBackoffStrategy (attempt
// count only). With neither registered the job retries immediately.
//
// This is the most flexible option: arbitrary formula, cap, and jitter
// all live in the registered Go function, so mk-go can reproduce
// Misskey's httpRelatedBackoff ((2^n-1)*base, capped at 8h, +0..20%
// jitter) verbatim.
func CustomBackoff() BackoffStrategy {
	return BackoffStrategy{Type: "custom"}
}

// computeBackoffDelay mirrors BullMQ's Backoffs.calculate worker-side
// computation for the built-in fixed / exponential strategies. It
// returns the un-jittered base delay; jitter (and custom strategies)
// are layered on by Worker.computeRetryDelay. The chosen delay is
// passed to Lua as a plain integer so the wire-level retry script
// (retryJob / moveToDelayed) does not know about strategy types.
//
// attemptsMade is the post-bump count (i.e. "this is attempt N");
// BullMQ's exponential formula is `delay * 2^(attemptsMade-1)`.
//
// Returns 0 for an unset / nil strategy or any non-built-in type.
// Overflow is clamped to math.MaxInt64 nanoseconds so a misconfigured
// exponential never wraps negative.
func computeBackoffDelay(b *BackoffStrategy, attemptsMade int) time.Duration {
	if b == nil || attemptsMade < 1 {
		return 0
	}
	switch b.Type {
	case "fixed":
		return b.Delay
	case "exponential":
		shift := uint(attemptsMade - 1)
		// 上限を 62 bit に抑える: 1<<63 は signed int64 で overflow。
		if shift >= 63 {
			return time.Duration(math.MaxInt64)
		}
		mult := time.Duration(1) << shift
		// b.Delay * mult が int64 を overflow しないかを乗算前にチェック。
		if mult != 0 && b.Delay > time.Duration(math.MaxInt64)/mult {
			return time.Duration(math.MaxInt64)
		}
		return b.Delay * mult
	default:
		return 0
	}
}

// applyJitter reproduces BullMQ's built-in jitter formula:
//
//	floor(random() * base * jitter + base * (1 - jitter))
//
// base is the un-jittered delay (delay for fixed, delay*2^(n-1) for
// exponential), r is a random value in [0, 1), and jitter is the
// fraction in (0, 1]. The result lands in [base*(1-jitter), base).
// BullMQ's Math.floor is reproduced by the float64 -> time.Duration
// truncation.
//
// jitter values outside (0, 1] are clamped so a misconfigured fraction
// never produces a negative or above-base delay.
func applyJitter(base time.Duration, jitter, r float64) time.Duration {
	if jitter <= 0 {
		return base
	}
	if jitter > 1 {
		jitter = 1
	}
	minDelay := float64(base) * (1 - jitter)
	return time.Duration(r*float64(base)*jitter + minDelay)
}

// RetryDelayFunc decides the delay before a failed job's next attempt,
// or declines to decide.
//
// Returning (d, true) uses d. Returning (_, false) leaves the decision
// to the job's configured backoff, exactly as if no override were
// registered — including the built-in fixed / exponential curves and
// their jitter.
//
// **「普段は設定どおり、この失敗のときだけ別の遅延」を書くためのもの。**
// BullMQ の settings.backoffStrategy (mkq では WithBackoffStrategy /
// WithBackoffStrategyFunc) は backoff type が custom のときしか呼ばれない
// ので、指数バックオフのキューで相手の Retry-After に従いたい、という形が
// 書けなかった。custom に切り替えればできるが、カーブ全体を持つことになる
// うえ `opts.backoff.type` が Redis に載って BullMQ TS 側の挙動まで変わる。
//
// Returning a negative duration stops the retries and fails the job,
// the same as a custom strategy doing so.
//
// Implementations must be safe for concurrent use: every dispatch
// goroutine calls this independently. A panic is caught, logged, and
// treated as a decline.
//
// **BullMQ に対応物は無い。** mkq の拡張で、API surface にしか現れない —
// 遅延は既存の moveToDelayed に整数ミリ秒で渡るだけなので、wire format は
// 変わらないし opts.backoff も書き換わらない。
type RetryDelayFunc func(BackoffContext) (time.Duration, bool)
