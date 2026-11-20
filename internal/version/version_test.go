package version

import (
	"runtime"
	"testing"
)

func TestStringDefaults(t *testing.T) {
	want := "tasklane dev (revision unknown, " + runtime.Version() + ")"
	if got := String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestStringUsesLinkedValues(t *testing.T) {
	oldV, oldR := Version, Revision
	t.Cleanup(func() { Version, Revision = oldV, oldR })
	Version, Revision = "1.2.3", "abc123"
	want := "tasklane 1.2.3 (revision abc123, " + runtime.Version() + ")"
	if got := String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
