// Package k8s holds tests for the Kubernetes deployment (design §20). They
// render the Helm chart with `helm template` and check the rules that keep
// restarts lossless and the pods locked down.
package k8s

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

type obj = map[string]any

// helmBin finds helm. CI's k8s-static job sets TASKLANE_REQUIRE_HELM so a
// missing helm fails there instead of skipping.
func helmBin(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("TASKLANE_REQUIRE_HELM") != "" {
			t.Fatal("helm not found and TASKLANE_REQUIRE_HELM is set")
		}
		t.Skip("helm not installed (deploy/k8s/tools.sh helm)")
	}
	return p
}

// helmTemplate runs `helm template tasklane ./tasklane -n tasklane` with
// extra arguments and returns stdout, stderr and the error.
func helmTemplate(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(helmBin(t), append([]string{"template", "tasklane", "./tasklane", "--namespace", "tasklane"}, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

// render renders the chart with a database URL (the chart requires one) plus
// args, and decodes every document.
func render(t *testing.T, args ...string) []obj {
	t.Helper()
	out, stderr, err := helmTemplate(t, append([]string{"--set", "databaseURL=postgres://u:p@db:5432/tasklane"}, args...)...)
	require.NoError(t, err, stderr)
	var objs []obj
	dec := yaml.NewDecoder(strings.NewReader(out))
	for {
		var o obj
		err := dec.Decode(&o)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if o != nil {
			objs = append(objs, o)
		}
	}
	return objs
}

func find(t *testing.T, objs []obj, kind, name string) obj {
	t.Helper()
	for _, o := range objs {
		if o["kind"] == kind && dig(o, "metadata", "name") == name {
			return o
		}
	}
	t.Fatalf("no %s %q rendered", kind, name)
	return nil
}

func has(objs []obj, kind, name string) bool {
	for _, o := range objs {
		if o["kind"] == kind && dig(o, "metadata", "name") == name {
			return true
		}
	}
	return false
}

// dig walks maps by string key and lists by int index; nil if absent.
func dig(v any, path ...any) any {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(obj)
			if !ok {
				return nil
			}
			v = m[k]
		case int:
			l, ok := v.([]any)
			if !ok || k >= len(l) {
				return nil
			}
			v = l[k]
		}
	}
	return v
}

func serverContainer(t *testing.T, objs []obj) obj {
	t.Helper()
	c, ok := dig(find(t, objs, "Deployment", "tasklane-server"), "spec", "template", "spec", "containers", 0).(obj)
	require.True(t, ok)
	return c
}

// The server's own drain (cmd/tasklane/main.go): HTTP 10 s + gRPC 10 s +
// metrics 2 s.
const serverDrainBudget = 22

func TestServerDrainFitsTheGracePeriod(t *testing.T) {
	objs := render(t)
	pod := dig(find(t, objs, "Deployment", "tasklane-server"), "spec", "template", "spec")
	grace := dig(pod, "terminationGracePeriodSeconds")
	sleep := dig(serverContainer(t, objs), "lifecycle", "preStop", "sleep", "seconds")
	require.Equal(t, 40, grace)
	require.Equal(t, 5, sleep)
	require.Less(t, sleep.(int)+serverDrainBudget, grace.(int), "preStop + drain must end before the hard kill")
}

func TestServerRollsOutWithoutLosingCapacity(t *testing.T) {
	d := find(t, render(t), "Deployment", "tasklane-server")
	require.Equal(t, 2, dig(d, "spec", "replicas"))
	require.Equal(t, "RollingUpdate", dig(d, "spec", "strategy", "type"))
	require.Equal(t, 0, dig(d, "spec", "strategy", "rollingUpdate", "maxUnavailable"))
	require.Equal(t, 1, dig(d, "spec", "strategy", "rollingUpdate", "maxSurge"))
}

func TestServerProbes(t *testing.T) {
	c := serverContainer(t, render(t))
	require.Equal(t, "/healthz", dig(c, "startupProbe", "httpGet", "path"))
	require.Equal(t, "/healthz", dig(c, "livenessProbe", "httpGet", "path"))
	require.Equal(t, "/readyz", dig(c, "readinessProbe", "httpGet", "path"))
	require.Equal(t, 2, dig(c, "readinessProbe", "periodSeconds"))
	require.Equal(t, 2, dig(c, "readinessProbe", "failureThreshold"))
}

func TestServerRunsLockedDown(t *testing.T) {
	objs := render(t)
	pod := dig(find(t, objs, "Deployment", "tasklane-server"), "spec", "template", "spec")
	require.Equal(t, true, dig(pod, "securityContext", "runAsNonRoot"))
	require.Equal(t, false, dig(pod, "automountServiceAccountToken"))
	c := serverContainer(t, objs)
	require.Equal(t, true, dig(c, "securityContext", "readOnlyRootFilesystem"))
	require.Equal(t, false, dig(c, "securityContext", "allowPrivilegeEscalation"))
	require.Equal(t, []any{"ALL"}, dig(c, "securityContext", "capabilities", "drop"))
	require.NotNil(t, dig(c, "resources", "limits", "memory"))
}

func TestSecretsStayOutOfPlainEnv(t *testing.T) {
	objs := render(t, "--set", "env.TASKLANE_REAPER_INTERVAL=2s", "--set", "apiKeys=k1")
	c := serverContainer(t, objs)
	env, _ := dig(c, "env").([]any)
	require.NotEmpty(t, env)
	for _, e := range env {
		name := dig(e, "name")
		require.NotEqual(t, "TASKLANE_DATABASE_URL", name)
		require.NotEqual(t, "TASKLANE_API_KEYS", name)
	}
	require.Equal(t, "tasklane-server", dig(c, "envFrom", 0, "secretRef", "name"))
	s := find(t, objs, "Secret", "tasklane-server")
	require.Equal(t, "postgres://u:p@db:5432/tasklane", dig(s, "stringData", "TASKLANE_DATABASE_URL"))
	require.Equal(t, "k1", dig(s, "stringData", "TASKLANE_API_KEYS"))

	objs = render(t, "--set", "existingSecret=mine")
	require.False(t, has(objs, "Secret", "tasklane-server"), "no Secret is made when one is supplied")
	require.Equal(t, "mine", dig(serverContainer(t, objs), "envFrom", 0, "secretRef", "name"))

	_, stderr, err := helmTemplate(t)
	require.Error(t, err, "neither databaseURL nor existingSecret")
	require.Contains(t, stderr, "databaseURL or existingSecret is required")
}

func TestServerPDBMatchesThePods(t *testing.T) {
	objs := render(t)
	pdb := find(t, objs, "PodDisruptionBudget", "tasklane-server")
	require.Equal(t, 1, dig(pdb, "spec", "maxUnavailable"))
	sel := dig(pdb, "spec", "selector", "matchLabels").(obj)
	labels := dig(find(t, objs, "Deployment", "tasklane-server"), "spec", "template", "metadata", "labels").(obj)
	for k, v := range sel {
		require.Equal(t, v, labels[k], "PDB selector %s", k)
	}
}

func TestHeadlessServiceListsOnlyReadyServers(t *testing.T) {
	s := find(t, render(t), "Service", "tasklane-headless")
	require.Equal(t, "None", dig(s, "spec", "clusterIP"))
	require.Equal(t, false, dig(s, "spec", "publishNotReadyAddresses"))
	require.Equal(t, "server", dig(s, "spec", "selector", "app.kubernetes.io/component"))
	var ports []any
	for _, p := range dig(s, "spec", "ports").([]any) {
		ports = append(ports, dig(p, "port"))
	}
	require.ElementsMatch(t, []any{7070, 8080, 9464}, ports)
}
