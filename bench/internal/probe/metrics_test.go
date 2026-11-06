package probe

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const sample = `# HELP tasklane_rpc_duration_seconds RPC duration.
# TYPE tasklane_rpc_duration_seconds histogram
tasklane_rpc_duration_seconds_bucket{code="OK",method="Lease",le="0.005"} 3
tasklane_rpc_duration_seconds_count{code="OK",method="Lease"} 120
tasklane_rpc_duration_seconds_count{code="Canceled",method="Lease"} 7
tasklane_rpc_duration_seconds_count{code="OK",method="Ack"} 900
tasklane_jobs_acked_total{queue="bench"} 900
`

func TestSumSamplesMatchesLabels(t *testing.T) {
	v, err := SumSamples(strings.NewReader(sample), "tasklane_rpc_duration_seconds_count", map[string]string{"method": "Lease"})
	require.NoError(t, err)
	require.Equal(t, 127.0, v, "summed over codes")
}

func TestSumSamplesWithoutLabels(t *testing.T) {
	v, err := SumSamples(strings.NewReader(sample), "tasklane_jobs_acked_total", nil)
	require.NoError(t, err)
	require.Equal(t, 900.0, v)
}

func TestSumSamplesDoesNotMatchAPrefix(t *testing.T) {
	v, err := SumSamples(strings.NewReader(sample), "tasklane_rpc_duration_seconds", nil)
	require.NoError(t, err)
	require.Zero(t, v, "_count and _bucket are different names")
}
