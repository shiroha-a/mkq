package proto

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestEncodeMoveToActiveOpts_Roundtrip(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeMoveToActiveOpts(MoveToActiveOpts{
		Token:        "tok",
		LockDuration: 30000,
		Name:         "worker-1",
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var m map[string]any
	if err := msgpack.Unmarshal(encoded, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m["token"] != "tok" {
		t.Errorf("token: got %v", m["token"])
	}
	if got, ok := toInt64(m["lockDuration"]); !ok || got != 30000 {
		t.Errorf("lockDuration: got %v", m["lockDuration"])
	}
	if m["name"] != "worker-1" {
		t.Errorf("name: got %v", m["name"])
	}
}

func TestEncodeMoveToActiveOpts_OmitsEmptyName(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeMoveToActiveOpts(MoveToActiveOpts{
		Token:        "tok",
		LockDuration: 1000,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var m map[string]any
	if err := msgpack.Unmarshal(encoded, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, present := m["name"]; present {
		t.Errorf("empty Name should be omitted, got %v", m["name"])
	}
}

func TestEncodeMoveToFinishedOpts_AlwaysIncludesNilSafeFields(t *testing.T) {
	t.Parallel()

	// Two fields the vendored Lua dereferences without nil guards:
	//   - opts['keepJobs']['count'] (moveToFinished-14.lua:104)
	//   - opts['maxMetricsSize'] in collectMetrics → tonumber(nil)
	//     crashes on the second invocation.
	encoded, err := EncodeMoveToFinishedOpts(MoveToFinishedOpts{
		Token:        "t",
		LockDuration: 1000,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var m map[string]any
	if err := msgpack.Unmarshal(encoded, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := m["keepJobs"]; !ok {
		t.Error("keepJobs must always be present in the encoded opts map")
	}
	v, ok := m["maxMetricsSize"]
	if !ok {
		t.Error("maxMetricsSize must always be present in the encoded opts map")
	} else if v != "" {
		t.Errorf("maxMetricsSize default should be empty string, got %v", v)
	}
}

func TestEncodeJobFields(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeJobFields("finishedOn", int64(1714000000000))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var arr []any
	if err := msgpack.Unmarshal(encoded, &arr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(arr) != 2 {
		t.Fatalf("expected flat [k,v] of length 2, got %d", len(arr))
	}
	if arr[0] != "finishedOn" {
		t.Errorf("key: got %v", arr[0])
	}
	if got, ok := toInt64(arr[1]); !ok || got != 1714000000000 {
		t.Errorf("value: got %v", arr[1])
	}
}

func TestEncodeJobFields_RejectsOdd(t *testing.T) {
	t.Parallel()
	if _, err := EncodeJobFields("k1", "v1", "k2"); err == nil {
		t.Fatal("expected error for odd argument count, got nil")
	}
}

func TestEncodeJobFields_EmptyReturnsNil(t *testing.T) {
	t.Parallel()
	got, err := EncodeJobFields()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("empty input should produce nil bytes, got %v", got)
	}
}
