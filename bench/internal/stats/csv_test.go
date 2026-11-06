package stats

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendRowWritesTheHeaderOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.csv")
	require.NoError(t, AppendRow(path, Row{Scenario: "s2", Label: "s2-closed-w10", Valid: true, Throughput: 812.5}))
	require.NoError(t, AppendRow(path, Row{Scenario: "s2", Label: "s2-closed-w50", Valid: false, Reason: "2.1% errors"}))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.Len(t, recs, 3, "one header and two rows")
	require.Equal(t, Header(), recs[0])
	require.Equal(t, "s2-closed-w10", recs[1][1])
	require.Equal(t, "true", recs[1][col("valid")])
	require.Equal(t, "812.5", recs[1][col("throughput_jobs_s")])
	require.Equal(t, "2.1% errors", recs[2][col("reason")])
}

func TestHeaderAndValuesHaveTheSameLength(t *testing.T) {
	require.Len(t, Row{}.Values(), len(Header()))
}

func col(name string) int {
	for i, h := range Header() {
		if h == name {
			return i
		}
	}
	panic("no column " + name)
}
