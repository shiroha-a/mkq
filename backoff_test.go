package mkq

import (
	"math"
	"testing"
	"time"
)

func TestComputeBackoffDelay_Nil(t *testing.T) {
	t.Parallel()
	if got := computeBackoffDelay(nil, 1); got != 0 {
		t.Errorf("nil strategy: expected 0, got %v", got)
	}
}

func TestComputeBackoffDelay_Fixed(t *testing.T) {
	t.Parallel()
	b := FixedBackoff(150 * time.Millisecond)
	for _, n := range []int{1, 2, 5, 100} {
		if got := computeBackoffDelay(&b, n); got != 150*time.Millisecond {
			t.Errorf("fixed attempt=%d: expected 150ms, got %v", n, got)
		}
	}
}

func TestComputeBackoffDelay_Exponential(t *testing.T) {
	t.Parallel()
	b := ExponentialBackoff(20 * time.Millisecond)
	cases := []struct {
		attemptsMade int
		want         time.Duration
	}{
		{1, 20 * time.Millisecond},     // 20 * 2^0
		{2, 40 * time.Millisecond},     // 20 * 2^1
		{3, 80 * time.Millisecond},     // 20 * 2^2
		{4, 160 * time.Millisecond},    // 20 * 2^3
		{10, 10240 * time.Millisecond}, // 20 * 2^9
	}
	for _, c := range cases {
		if got := computeBackoffDelay(&b, c.attemptsMade); got != c.want {
			t.Errorf("exp attempt=%d: expected %v, got %v", c.attemptsMade, c.want, got)
		}
	}
}

func TestComputeBackoffDelay_ExponentialOverflow(t *testing.T) {
	t.Parallel()
	// 1 second base, attempt=70: 1s * 2^69 ≈ 5.9e20 ns, way past int64.
	b := ExponentialBackoff(1 * time.Second)
	got := computeBackoffDelay(&b, 70)
	if got != time.Duration(math.MaxInt64) {
		t.Errorf("expected clamped MaxInt64, got %v", got)
	}

	// Boundary: shift==63 should clamp without underflow.
	if got := computeBackoffDelay(&b, 64); got != time.Duration(math.MaxInt64) {
		t.Errorf("attempt=64 should clamp, got %v", got)
	}
}

func TestComputeBackoffDelay_UnknownType(t *testing.T) {
	t.Parallel()
	b := BackoffStrategy{Type: "polynomial", Delay: time.Second}
	if got := computeBackoffDelay(&b, 3); got != 0 {
		t.Errorf("unknown type should return 0, got %v", got)
	}
}

func TestComputeBackoffDelay_AttemptsZeroOrNegative(t *testing.T) {
	t.Parallel()
	b := ExponentialBackoff(time.Second)
	if got := computeBackoffDelay(&b, 0); got != 0 {
		t.Errorf("attempt=0 should return 0, got %v", got)
	}
	if got := computeBackoffDelay(&b, -1); got != 0 {
		t.Errorf("attempt=-1 should return 0, got %v", got)
	}
}

func TestBackoffConstructors(t *testing.T) {
	t.Parallel()
	if b := FixedBackoffWithJitter(2*time.Second, 0.3); b.Type != "fixed" || b.Delay != 2*time.Second || b.Jitter != 0.3 {
		t.Errorf("FixedBackoffWithJitter: got %+v", b)
	}
	if b := ExponentialBackoffWithJitter(time.Second, 0.2); b.Type != "exponential" || b.Delay != time.Second || b.Jitter != 0.2 {
		t.Errorf("ExponentialBackoffWithJitter: got %+v", b)
	}
	if b := CustomBackoff(); b.Type != "custom" || b.Delay != 0 || b.Jitter != 0 {
		t.Errorf("CustomBackoff: got %+v", b)
	}
}

func TestApplyJitter(t *testing.T) {
	t.Parallel()
	base := time.Duration(1000)
	cases := []struct {
		name   string
		jitter float64
		r      float64
		want   time.Duration
	}{
		// floor(r*base*jitter + base*(1-jitter))
		{"r=0 yields minDelay", 0.2, 0.0, 800},
		{"r=0.5 mid", 0.2, 0.5, 900},
		{"jitter=0 returns base", 0.0, 0.99, 1000},
		{"jitter clamped above 1", 5.0, 0.0, 0}, // base*(1-1)=0
		{"full jitter r=0", 1.0, 0.0, 0},
	}
	for _, c := range cases {
		if got := applyJitter(base, c.jitter, c.r); got != c.want {
			t.Errorf("%s: applyJitter(%v,%v,%v)=%v want %v", c.name, base, c.jitter, c.r, got, c.want)
		}
	}

	// r in [0,1) keeps the result within [minDelay, base) for jitter in (0,1].
	for _, r := range []float64{0, 0.25, 0.5, 0.75, 0.999} {
		got := applyJitter(10000, 0.2, r)
		if got < 8000 || got >= 10000 {
			t.Errorf("jittered delay out of range for r=%v: got %v", r, got)
		}
	}
}

func TestComputeRetryDelay_BuiltinNoJitter(t *testing.T) {
	t.Parallel()
	// rng must not be consulted when Jitter == 0.
	w := &Worker{rng: func() float64 { t.Fatal("rng called without jitter"); return 0 }}
	fixed := FixedBackoff(150 * time.Millisecond)
	if got := w.computeRetryDelay(&fixed, 5); got != 150*time.Millisecond {
		t.Errorf("fixed: got %v", got)
	}
	exp := ExponentialBackoff(20 * time.Millisecond)
	if got := w.computeRetryDelay(&exp, 3); got != 80*time.Millisecond {
		t.Errorf("exp attempt=3: got %v want 80ms", got)
	}
}

func TestComputeRetryDelay_BuiltinJitter(t *testing.T) {
	t.Parallel()
	w := &Worker{rng: func() float64 { return 0.5 }}
	exp := ExponentialBackoffWithJitter(100*time.Millisecond, 0.2)
	// attempt 3: base = 100ms * 2^2 = 400ms.
	// jitter: 0.5*400*0.2 + 400*0.8 = 40 + 320 = 360ms.
	if got := w.computeRetryDelay(&exp, 3); got != 360*time.Millisecond {
		t.Errorf("exp+jitter: got %v want 360ms", got)
	}
}

func TestComputeRetryDelay_Custom(t *testing.T) {
	t.Parallel()
	w := &Worker{backoffStrategy: func(attemptsMade int) time.Duration {
		return time.Duration(attemptsMade) * time.Second
	}}
	b := CustomBackoff()
	if got := w.computeRetryDelay(&b, 4); got != 4*time.Second {
		t.Errorf("custom: got %v want 4s", got)
	}

	// Unregistered custom strategy falls back to immediate retry.
	w2 := &Worker{}
	if got := w2.computeRetryDelay(&b, 4); got != 0 {
		t.Errorf("unregistered custom: got %v want 0", got)
	}

	// Unknown (non-built-in) type with no strategy also returns 0.
	unknown := BackoffStrategy{Type: "polynomial", Delay: time.Second}
	if got := w2.computeRetryDelay(&unknown, 3); got != 0 {
		t.Errorf("unknown type: got %v want 0", got)
	}
}

func TestComputeRetryDelay_NilAndZeroAttempts(t *testing.T) {
	t.Parallel()
	w := &Worker{}
	if got := w.computeRetryDelay(nil, 3); got != 0 {
		t.Errorf("nil strategy: got %v", got)
	}
	b := ExponentialBackoff(time.Second)
	if got := w.computeRetryDelay(&b, 0); got != 0 {
		t.Errorf("attempt=0: got %v", got)
	}
}

// TestComputeRetryDelay_MisskeyHttpRelatedBackoff verifies a Worker can
// reproduce Misskey's httpRelatedBackoff ((2^n - 1) * base, capped at
// 8h) via a registered custom strategy. Jitter is exercised separately;
// here r is fixed to 0 so the assertions are deterministic.
func TestComputeRetryDelay_MisskeyHttpRelatedBackoff(t *testing.T) {
	t.Parallel()
	const base = time.Minute
	const cap = 8 * time.Hour
	w := &Worker{backoffStrategy: func(attemptsMade int) time.Duration {
		d := time.Duration(math.Pow(2, float64(attemptsMade))-1) * base
		if d > cap {
			d = cap
		}
		return d
	}}
	b := CustomBackoff()
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Minute}, // (2^1-1)*60s = 60s
		{2, 3 * time.Minute}, // (2^2-1)*60s = 180s
		{3, 7 * time.Minute}, // (2^3-1)*60s = 420s
		{12, 8 * time.Hour},  // (2^12-1)*60s = 68.25h -> capped 8h
		{20, 8 * time.Hour},  // well past the cap
	}
	for _, c := range cases {
		if got := w.computeRetryDelay(&b, c.attempt); got != c.want {
			t.Errorf("attempt=%d: got %v want %v", c.attempt, got, c.want)
		}
	}
}
