package probe

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCgroupCPUReadsUsage(t *testing.T) {
	dir := t.TempDir()
	stat := "usage_usec 1234567\nuser_usec 1000000\nsystem_usec 234567\nnr_periods 0\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cpu.stat"), []byte(stat), 0o644))
	d, err := CgroupCPU(dir)
	require.NoError(t, err)
	require.Equal(t, 1234567*time.Microsecond, d)
}

func TestCgroupCPUFailsWithoutUsage(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cpu.stat"), []byte("nr_periods 0\n"), 0o644))
	_, err := CgroupCPU(dir)
	require.ErrorContains(t, err, "usage_usec")
}
