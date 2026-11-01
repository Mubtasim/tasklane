//go:build chaos

package chaos

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// stack drives the demo stack with the chaos override through docker
// compose. Every command is scoped to one project name, so the test never
// touches other containers on the machine.
type stack struct {
	t       *testing.T
	project string
	files   []string
	env     []string
}

func newStack(t *testing.T, ledgerDir string) *stack {
	return &stack{
		t:       t,
		project: "tasklane-chaos",
		files:   []string{"../../deploy/docker-compose.yml", "../../deploy/chaos.override.yml"},
		env:     append(os.Environ(), "CHAOS_LEDGER_DIR="+ledgerDir),
	}
}

// run executes one compose command and returns its combined output.
func (s *stack) run(args ...string) (string, error) {
	full := []string{"compose", "-p", s.project}
	for _, f := range s.files {
		full = append(full, "-f", f)
	}
	full = append(full, args...)
	cmd := exec.Command("docker", full...)
	cmd.Env = s.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker compose %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return string(out), nil
}

func (s *stack) up() error { _, err := s.run("up", "--build", "-d"); return err }

// down removes the containers and their volumes, so the next run starts
// from an empty database.
func (s *stack) down() error { _, err := s.run("down", "-v", "--remove-orphans"); return err }

func (s *stack) kill(svc ...string) error {
	_, err := s.run(append([]string{"kill"}, svc...)...)
	return err
}

func (s *stack) start(svc ...string) error {
	_, err := s.run(append([]string{"start"}, svc...)...)
	return err
}

func (s *stack) pause(svc ...string) error {
	_, err := s.run(append([]string{"pause"}, svc...)...)
	return err
}

func (s *stack) unpause(svc ...string) error {
	_, err := s.run(append([]string{"unpause"}, svc...)...)
	return err
}

func (s *stack) restart(svc ...string) error {
	_, err := s.run(append([]string{"restart"}, svc...)...)
	return err
}

func (s *stack) stop(svc ...string) error {
	_, err := s.run(append([]string{"stop"}, svc...)...)
	return err
}

// logs writes every service's logs, with timestamps, to path.
func (s *stack) logs(path string) error {
	out, err := s.run("logs", "--no-color", "--timestamps")
	if werr := os.WriteFile(path, []byte(out), 0o644); werr != nil && err == nil {
		err = werr
	}
	return err
}
