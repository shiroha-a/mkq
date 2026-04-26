package mkq

import (
	"testing"
	"time"
)

// These tests pin parseJobOpts against the polymorphic shapes BullMQ
// TS persists into the per-job HASH `opts` JSON. The earlier strict
// `*int` decoding silently zeroed out attempts/backoff/lifo whenever
// a foreign worker stored e.g. `removeOnComplete: true` — a wire
// compatibility bug spotted in PR #15 review.

func TestParseJobOpts_RemoveOnComplete_BooleanTrue(t *testing.T) {
	t.Parallel()
	got := parseJobOpts(`{"attempts":3,"removeOnComplete":true}`)
	if got.attempts != 3 {
		t.Errorf("attempts must survive boolean removeOnComplete, got %d", got.attempts)
	}
	if got.removeOnComplete == nil || *got.removeOnComplete != 0 {
		t.Errorf("removeOnComplete=true should map to *int(0), got %v", got.removeOnComplete)
	}
}

func TestParseJobOpts_RemoveOnComplete_BooleanFalse(t *testing.T) {
	t.Parallel()
	got := parseJobOpts(`{"attempts":2,"removeOnComplete":false}`)
	if got.attempts != 2 {
		t.Errorf("attempts must survive boolean removeOnComplete, got %d", got.attempts)
	}
	if got.removeOnComplete != nil {
		t.Errorf("removeOnComplete=false should map to nil, got %v", *got.removeOnComplete)
	}
}

func TestParseJobOpts_RemoveOnComplete_Number(t *testing.T) {
	t.Parallel()
	got := parseJobOpts(`{"removeOnComplete":7}`)
	if got.removeOnComplete == nil || *got.removeOnComplete != 7 {
		t.Errorf("removeOnComplete=7 should map to *int(7), got %v", got.removeOnComplete)
	}
}

func TestParseJobOpts_RemoveOnComplete_Object(t *testing.T) {
	t.Parallel()
	got := parseJobOpts(`{"removeOnComplete":{"count":42,"age":3600}}`)
	if got.removeOnComplete == nil || *got.removeOnComplete != 42 {
		t.Errorf("removeOnComplete={count:42} should map to *int(42), got %v", got.removeOnComplete)
	}
}

func TestParseJobOpts_RemoveOnFail_BooleanForms(t *testing.T) {
	t.Parallel()
	if got := parseJobOpts(`{"removeOnFail":true}`); got.removeOnFail == nil || *got.removeOnFail != 0 {
		t.Errorf("removeOnFail=true should map to *int(0), got %v", got.removeOnFail)
	}
	if got := parseJobOpts(`{"removeOnFail":false}`); got.removeOnFail != nil {
		t.Errorf("removeOnFail=false should map to nil")
	}
}

func TestParseJobOpts_Backoff_NumberShorthand(t *testing.T) {
	t.Parallel()
	got := parseJobOpts(`{"attempts":5,"backoff":2000}`)
	if got.attempts != 5 {
		t.Errorf("attempts must survive shorthand backoff, got %d", got.attempts)
	}
	if got.backoff == nil {
		t.Fatal("backoff number shorthand must be parsed")
	}
	if got.backoff.Type != "fixed" {
		t.Errorf("number-shorthand backoff should have Type=fixed, got %q", got.backoff.Type)
	}
	if got.backoff.Delay != 2*time.Second {
		t.Errorf("number-shorthand backoff Delay should be 2s, got %v", got.backoff.Delay)
	}
}

func TestParseJobOpts_Backoff_ObjectForm(t *testing.T) {
	t.Parallel()
	got := parseJobOpts(`{"backoff":{"type":"exponential","delay":500}}`)
	if got.backoff == nil {
		t.Fatal("object backoff must be parsed")
	}
	if got.backoff.Type != "exponential" || got.backoff.Delay != 500*time.Millisecond {
		t.Errorf("object backoff parsed incorrectly: %+v", got.backoff)
	}
}

func TestParseJobOpts_AllForeignFieldsTogether(t *testing.T) {
	t.Parallel()
	// Realistic foreign-worker shape: backoff as number, removeOn*
	// as boolean, alongside attempts / lifo. The earlier strict
	// decoder zeroed every field; the new code must preserve them.
	raw := `{"attempts":4,"lifo":true,"backoff":1000,"removeOnComplete":true,"removeOnFail":10}`
	got := parseJobOpts(raw)
	if got.attempts != 4 {
		t.Errorf("attempts: %d", got.attempts)
	}
	if !got.lifo {
		t.Error("lifo must be true")
	}
	if got.backoff == nil || got.backoff.Type != "fixed" || got.backoff.Delay != time.Second {
		t.Errorf("backoff: %+v", got.backoff)
	}
	if got.removeOnComplete == nil || *got.removeOnComplete != 0 {
		t.Errorf("removeOnComplete: %v", got.removeOnComplete)
	}
	if got.removeOnFail == nil || *got.removeOnFail != 10 {
		t.Errorf("removeOnFail: %v", got.removeOnFail)
	}
}

func TestParseJobOpts_GarbageRemoveOpt(t *testing.T) {
	t.Parallel()
	// String / array / unrecognised should not poison sibling fields.
	got := parseJobOpts(`{"attempts":2,"removeOnComplete":"weird","removeOnFail":[1,2]}`)
	if got.attempts != 2 {
		t.Errorf("attempts must survive garbage removeOn*, got %d", got.attempts)
	}
	if got.removeOnComplete != nil {
		t.Errorf("garbage string should yield nil, got %v", *got.removeOnComplete)
	}
	if got.removeOnFail != nil {
		t.Errorf("garbage array should yield nil, got %v", *got.removeOnFail)
	}
}

func TestParseJobOpts_EmptyAndMalformed(t *testing.T) {
	t.Parallel()
	cases := []string{"", "{}", "not json"}
	for _, raw := range cases {
		got := parseJobOpts(raw)
		if got.attempts != 0 || got.backoff != nil || got.lifo {
			t.Errorf("%q should yield zero jobOpts, got %+v", raw, got)
		}
	}
}
