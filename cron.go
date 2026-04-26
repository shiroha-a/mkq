package mkq

import (
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// cronParser is the strict 5-field parser instance. mkq's documented
// subset matches BullMQ's cron-parser@4.9.0 mainstream syntax —
// `Descriptor` enables `@yearly` / `@daily` / etc. but `WithSeconds`
// is intentionally absent (Quartz mode would flip DOM/DOW from Vixie
// OR to AND, breaking BullMQ compatibility).
//
// `@every <duration>` is a robfig-only extension not supported by
// cron-parser; mkq routes that use case through WithScheduleEvery
// instead. Any expression robfig parses but cron-parser would reject
// silently diverges, so the supported subset is documented and
// validated explicitly via parseCron.
var cronParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// cronCache memoises compiled cron schedules keyed by pattern+tz.
// Schedules are immutable post-compile and shared safely across
// goroutines; the cache leaks across the process lifetime by design
// — pattern strings in a typical app are finite and small.
var cronCache sync.Map // key: "<pattern>|<tz>" -> *cachedCron

type cachedCron struct {
	schedule cron.Schedule
	loc      *time.Location
}

// parseCron compiles `pattern` (under the IANA `tz`, empty = local)
// and caches the result. `tz` is validated via time.LoadLocation,
// which rejects unknown zones; `pattern` is validated via cronParser
// which surfaces the underlying robfig error verbatim.
//
// Rejected up front (before robfig sees them) so the error message
// can name the unsupported extension instead of "unrecognized
// descriptor":
//   - `@every <dur>` (robfig accepts it; cron-parser does not)
func parseCron(pattern, tz string) (*cachedCron, error) {
	if pattern == "" {
		return nil, fmt.Errorf("mkq: pattern must be non-empty")
	}
	if isAtEvery(pattern) {
		return nil, fmt.Errorf("mkq: %q is not supported as a cron pattern; use WithScheduleEvery for fixed intervals", pattern)
	}

	key := pattern + "|" + tz
	if v, ok := cronCache.Load(key); ok {
		return v.(*cachedCron), nil
	}

	loc := time.Local
	if tz != "" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			return nil, fmt.Errorf("mkq: invalid timezone %q: %w", tz, err)
		}
		loc = l
	}

	sched, err := cronParser.Parse(pattern)
	if err != nil {
		return nil, fmt.Errorf("mkq: invalid cron pattern %q: %w", pattern, err)
	}

	cc := &cachedCron{schedule: sched, loc: loc}
	actual, _ := cronCache.LoadOrStore(key, cc)
	return actual.(*cachedCron), nil
}

// nextFire returns the next fire time at or after `from`, in the
// schedule's timezone.
func (c *cachedCron) nextFire(from time.Time) time.Time {
	return c.schedule.Next(from.In(c.loc))
}

// isAtEvery detects `@every <duration>` so the rejection error can
// be specific. Case-insensitive prefix check, mirroring cron-parser's
// "@every" non-support stance.
func isAtEvery(s string) bool {
	const prefix = "@every"
	if len(s) < len(prefix) {
		return false
	}
	for i := range len(prefix) {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != prefix[i] {
			return false
		}
	}
	return true
}
