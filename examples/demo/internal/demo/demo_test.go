package demo

import (
	"testing"
	"time"
)

func TestLoadConfigVisibilityTimeout(t *testing.T) {
	for _, tc := range []struct {
		name, val string
		want      time.Duration
	}{
		{"valid", "3s", 3 * time.Second},
		{"unset", "", 15 * time.Second},
		{"malformed", "soon", 15 * time.Second},
		{"non-positive", "-2s", 15 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DEMO_VISIBILITY_TIMEOUT", tc.val)
			if got := LoadConfig().VisibilityTimeout; got != tc.want {
				t.Fatalf("VisibilityTimeout = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLoadConfigLedgerDir(t *testing.T) {
	t.Setenv("DEMO_LEDGER_DIR", "")
	if got := LoadConfig().LedgerDir; got != "" {
		t.Fatalf("LedgerDir = %q, want empty", got)
	}
	t.Setenv("DEMO_LEDGER_DIR", "/tmp/ledgers")
	if got := LoadConfig().LedgerDir; got != "/tmp/ledgers" {
		t.Fatalf("LedgerDir = %q", got)
	}
}
