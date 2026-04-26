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
