package proto

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestEncodeAddArgs_RoundtripsViaMsgpack(t *testing.T) {
	t.Parallel()

	args := AddArgs{
		Prefix:    "bull:deliver:",
		CustomID:  "",
		Name:      "deliver",
		Timestamp: 1714000000000,
	}
	encoded, err := EncodeAddArgs(args)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var decoded []any
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, want := len(decoded), 9; got != want {
		t.Fatalf("array length: got %d, want %d (BullMQ args[1..9])", got, want)
	}
	if decoded[0] != "bull:deliver:" {
		t.Errorf("args[1] prefix: got %v", decoded[0])
	}
	if decoded[1] != "" {
		t.Errorf("args[2] customID should be empty string, got %v (%T)", decoded[1], decoded[1])
	}
	if decoded[2] != "deliver" {
		t.Errorf("args[3] name: got %v", decoded[2])
	}
	// 数値型の幅はmsgpack側で自動選択されるので、値だけ比較する。
	if got, ok := toInt64(decoded[3]); !ok || got != 1714000000000 {
		t.Errorf("args[4] timestamp: got %v (%T)", decoded[3], decoded[3])
	}
	for i := 4; i < 9; i++ {
		if decoded[i] != nil {
			t.Errorf("args[%d] should be nil, got %v", i+1, decoded[i])
		}
	}
}

func TestEncodeAddOpts_OmitsZeroValues(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeAddOpts(AddOpts{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded map[string]any
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("zero AddOpts should produce empty map, got %v", decoded)
	}
}

func TestEncodeAddOpts_FieldsRoundtrip(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeAddOpts(AddOpts{
		Delay:    5000,
		Priority: 7,
		Attempts: 3,
		Backoff:  &Backoff{Type: "exponential", Delay: 1000},
		Lifo:     true,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded map[string]any
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got, ok := toInt64(decoded["delay"]); !ok || got != 5000 {
		t.Errorf("delay: got %v", decoded["delay"])
	}
	if got, ok := toInt64(decoded["priority"]); !ok || got != 7 {
		t.Errorf("priority: got %v", decoded["priority"])
	}
	if got, ok := toInt64(decoded["attempts"]); !ok || got != 3 {
		t.Errorf("attempts: got %v", decoded["attempts"])
	}
	if decoded["lifo"] != true {
		t.Errorf("lifo: got %v", decoded["lifo"])
	}
	bo, ok := decoded["backoff"].(map[string]any)
	if !ok {
		t.Fatalf("backoff should be map, got %T: %v", decoded["backoff"], decoded["backoff"])
	}
	if bo["type"] != "exponential" {
		t.Errorf("backoff.type: got %v", bo["type"])
	}
	if got, ok := toInt64(bo["delay"]); !ok || got != 1000 {
		t.Errorf("backoff.delay: got %v", bo["delay"])
	}
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	case int:
		return int64(n), true
	case uint64:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint8:
		return int64(n), true
	}
	return 0, false
}
