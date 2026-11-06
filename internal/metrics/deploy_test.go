package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"

	"github.com/mubtasim/tasklane/internal/store"
)

var metricName = regexp.MustCompile(`\btasklane_[a-z_]+`)

// knownNames returns every metric name the registry exposes once each
// recording method has run.
func knownNames(t *testing.T) map[string]bool {
	m := New()
	// A lazy pool never dials (no MinConns), but gives WatchPool real Stats.
	pool, err := pgxpool.New(t.Context(), "postgres://u:p@127.0.0.1:1/db")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	m.WatchPool(pool.Stat)
	m.SetQueueStats([]store.QueueDepth{{Queue: "q"}})
	m.Leased([]store.Job{{Queue: "q", LeasedAt: time.Now()}})
	m.Pruned(PrunedDead, 1)
	_, _ = m.UnaryInterceptor()(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: "/s/M"}, func(context.Context, any) (any, error) { return nil, nil })
	families, err := m.reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	return names
}

// usedNames finds tasklane_* names in text, mapping histogram series
// (_bucket, _count, _sum) to their family.
func usedNames(text string) []string {
	var out []string
	for _, n := range metricName.FindAllString(text, -1) {
		for _, suffix := range []string{"_bucket", "_count", "_sum"} {
			if base, ok := strings.CutSuffix(n, suffix); ok && strings.HasSuffix(base, "_seconds") {
				n = base
			}
		}
		out = append(out, n)
	}
	return out
}

func TestDashboardUsesOnlyRealMetrics(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/grafana/dashboards/tasklane.json")
	require.NoError(t, err)
	var dash struct {
		UID    string `json:"uid"`
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	require.NoError(t, json.Unmarshal(raw, &dash))
	require.Equal(t, "tasklane", dash.UID)
	require.NotEmpty(t, dash.Panels)
	known := knownNames(t)
	for _, p := range dash.Panels {
		require.NotEmpty(t, p.Targets, "panel %q has no query", p.Title)
		for _, tg := range p.Targets {
			require.NotEmpty(t, tg.Expr, "panel %q has an empty query", p.Title)
			for _, n := range usedNames(tg.Expr) {
				require.True(t, known[n], "panel %q uses unknown metric %s", p.Title, n)
			}
		}
	}
}

func TestAlertRulesUseOnlyRealMetrics(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/prometheus/alerts.yml")
	require.NoError(t, err)
	known := knownNames(t)
	names := usedNames(string(raw))
	require.NotEmpty(t, names)
	for _, n := range names {
		require.True(t, known[n], "alerts.yml uses unknown metric %s", n)
	}
}

func TestDemoWiresTracing(t *testing.T) {
	read := func(p string) string {
		b, err := os.ReadFile("../../deploy/" + p)
		require.NoError(t, err)
		return string(b)
	}
	compose := read("docker-compose.yml")
	require.Regexp(t, `image: jaegertracing/jaeger:2\.\d+\.\d+`, compose, "Jaeger pinned to an exact v2 version")
	require.Contains(t, compose, "--enable-feature=exemplar-storage")
	require.Contains(t, compose, "OTEL_EXPORTER_OTLP_ENDPOINT: http://jaeger:4317")
	require.Contains(t, compose, `127.0.0.1:16686:16686`)
	require.Contains(t, read("jaeger/config.yaml"), "max_traces: 10000")

	jaegerDS := read("grafana/provisioning/datasources/jaeger.yml")
	require.Contains(t, jaegerDS, "type: jaeger")
	require.Contains(t, jaegerDS, "uid: jaeger")
	require.Contains(t, read("grafana/provisioning/datasources/prometheus.yml"), "datasourceUid: jaeger")

	raw, err := os.ReadFile("../../deploy/grafana/dashboards/tasklane.json")
	require.NoError(t, err)
	var dash struct {
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Exemplar bool `json:"exemplar"`
			} `json:"targets"`
		} `json:"panels"`
	}
	require.NoError(t, json.Unmarshal(raw, &dash))
	withExemplars := 0
	for _, p := range dash.Panels {
		if strings.HasPrefix(p.Title, "Queue wait") || strings.HasPrefix(p.Title, "RPC latency") {
			for _, tg := range p.Targets {
				require.True(t, tg.Exemplar, "panel %q target without exemplars", p.Title)
			}
			withExemplars++
		}
	}
	require.Equal(t, 2, withExemplars)
}

// k8sDocs decodes every YAML document in a demo manifest.
func k8sDocs(t *testing.T, name string) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../deploy/k8s/demo/" + name)
	require.NoError(t, err)
	var docs []map[string]any
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	for {
		var d map[string]any
		err := dec.Decode(&d)
		if errors.Is(err, io.EOF) {
			return docs
		}
		require.NoError(t, err)
		if d != nil {
			docs = append(docs, d)
		}
	}
}

func k8sDoc(t *testing.T, docs []map[string]any, kind string) map[string]any {
	t.Helper()
	for _, d := range docs {
		if d["kind"] == kind {
			return d
		}
	}
	t.Fatalf("no %s", kind)
	return nil
}

func TestScaledObjectUsesOnlyRealMetrics(t *testing.T) {
	docs := k8sDocs(t, "worker.yaml")
	so := k8sDoc(t, docs, "ScaledObject")
	spec := so["spec"].(map[string]any)
	require.Equal(t, 0, spec["minReplicaCount"])
	require.Equal(t, 5, spec["maxReplicaCount"])
	require.Equal(t, 5, spec["pollingInterval"])
	require.Equal(t, 30, spec["cooldownPeriod"])
	trigger := spec["triggers"].([]any)[0].(map[string]any)
	require.Equal(t, "prometheus", trigger["type"])
	md := trigger["metadata"].(map[string]any)
	require.Equal(t, "50", md["threshold"])
	require.Equal(t, "0", md["activationThreshold"])
	query := md["query"].(string)
	require.Contains(t, query, `state=~"available|leased"`, "running jobs count, or KEDA removes busy workers")
	require.Contains(t, query, "max by (state)", "both servers export the same gauge")
	known := knownNames(t)
	names := usedNames(query)
	require.NotEmpty(t, names)
	for _, n := range names {
		require.True(t, known[n], "ScaledObject uses unknown metric %s", n)
	}
}

// The worker pod must outlive the SDK's shutdown: ShutdownTimeout (25 s) plus
// the 2 s Nack budget (design §20.3).
func TestDemoWorkerGracePeriodCoversTheSDKDrain(t *testing.T) {
	d := k8sDoc(t, k8sDocs(t, "worker.yaml"), "Deployment")
	pod := d["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	require.Equal(t, 35, pod["terminationGracePeriodSeconds"])
	require.Greater(t, pod["terminationGracePeriodSeconds"].(int), 25+2)
}
