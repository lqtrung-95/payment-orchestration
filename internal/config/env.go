package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// loader accumulates errors while reading the environment so that boot reports
// every misconfigured value at once, rather than failing on the first one and
// forcing the operator through a fix-restart-repeat loop.
type loader struct {
	errs []string
}

func (l *loader) fail(key, reason string) {
	l.errs = append(l.errs, fmt.Sprintf("%s: %s", key, reason))
}

// err returns all accumulated problems as a single error, or nil if clean.
func (l *loader) err() error {
	if len(l.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(l.errs, "\n  - "))
}

// str reads an optional string, falling back to def when unset or empty.
func (l *loader) str(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// required reads a string that has no sensible default. Missing values are
// recorded rather than returned, so all of them surface in one error.
func (l *loader) required(key string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		l.fail(key, "is required but not set")
	}
	return v
}

func (l *loader) int(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		l.fail(key, fmt.Sprintf("must be an integer, got %q", raw))
		return def
	}
	return v
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		l.fail(key, fmt.Sprintf("must be a duration such as 5s or 200ms, got %q", raw))
		return def
	}
	return v
}

func (l *loader) bool(key string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		l.fail(key, fmt.Sprintf("must be a boolean, got %q", raw))
		return def
	}
	return v
}

// csv reads a comma-separated list, trimming whitespace around each element.
func (l *loader) csv(key string, def []string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		l.fail(key, "must contain at least one non-empty value")
		return def
	}
	return out
}

// oneOf reads a string constrained to a fixed set of allowed values.
func (l *loader) oneOf(key, def string, allowed ...string) string {
	v := l.str(key, def)
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	l.fail(key, fmt.Sprintf("must be one of %s, got %q", strings.Join(allowed, ", "), v))
	return def
}
