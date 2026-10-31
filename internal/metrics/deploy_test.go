package metrics

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
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
