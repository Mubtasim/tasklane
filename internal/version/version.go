// Package version holds the build's version, set at link time by the
// Dockerfile and the release workflow (design §22.4):
//
//	go build -ldflags "-X github.com/mubtasim/tasklane/internal/version.Version=1.0.0 \
//	  -X github.com/mubtasim/tasklane/internal/version.Revision=<commit>"
//
// Local builds keep the defaults.
package version

import "runtime"

var (
	Version  = "dev"
	Revision = "unknown"
)

// String is what `tasklane -version` prints.
func String() string {
	return "tasklane " + Version + " (revision " + Revision + ", " + runtime.Version() + ")"
}
