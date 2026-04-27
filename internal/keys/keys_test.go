package keys

import "testing"

func TestBuilder_BullMQLayout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"base", New("bull", "deliver").Base(), "bull:deliver:"},
		{"wait", New("bull", "deliver").Wait(), "bull:deliver:wait"},
		{"active", New("bull", "deliver").Active(), "bull:deliver:active"},
		{"delayed", New("bull", "deliver").Delayed(), "bull:deliver:delayed"},
		{"prioritized", New("bull", "deliver").Prioritized(), "bull:deliver:prioritized"},
		{"completed", New("bull", "deliver").Completed(), "bull:deliver:completed"},
		{"failed", New("bull", "deliver").Failed(), "bull:deliver:failed"},
		{"paused", New("bull", "deliver").Paused(), "bull:deliver:paused"},
		{"stalled", New("bull", "deliver").Stalled(), "bull:deliver:stalled"},
		{"stalled-check", New("bull", "deliver").StalledCheck(), "bull:deliver:stalled-check"},
		{"meta", New("bull", "deliver").Meta(), "bull:deliver:meta"},
		{"id", New("bull", "deliver").ID(), "bull:deliver:id"},
		{"marker", New("bull", "deliver").Marker(), "bull:deliver:marker"},
		{"pc", New("bull", "deliver").PriorityCounter(), "bull:deliver:pc"},
		{"events", New("bull", "deliver").Events(), "bull:deliver:events"},
		{"limiter", New("bull", "deliver").Limiter(), "bull:deliver:limiter"},
		{"repeat", New("bull", "deliver").Repeat(), "bull:deliver:repeat"},
		{"schedule", New("bull", "deliver").Schedule("daily"), "bull:deliver:repeat:daily"},
		{"dedup", New("bull", "deliver").Dedup("user:42"), "bull:deliver:de:user:42"},
		{"job", New("bull", "deliver").Job("42"), "bull:deliver:42"},
		{"job-uint", New("bull", "deliver").JobUint(42), "bull:deliver:42"},
		{"job-logs", New("bull", "deliver").JobLogs("42"), "bull:deliver:42:logs"},
		{"job-lock", New("bull", "deliver").JobLock("42"), "bull:deliver:42:lock"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, c.got, c.want)
		}
	}
}

func TestBuilder_DefaultPrefix(t *testing.T) {
	t.Parallel()
	if got := New("", "deliver").Wait(); got != "bull:deliver:wait" {
		t.Errorf("empty prefix should default to %q, got %q", DefaultPrefix, got)
	}
}

func TestBuilder_CustomPrefix(t *testing.T) {
	t.Parallel()
	if got := New("host", "deliver").Wait(); got != "host:deliver:wait" {
		t.Errorf("custom prefix not applied: %q", got)
	}
}
