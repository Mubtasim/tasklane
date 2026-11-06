package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseConfigDefaults(t *testing.T) {
	c, err := parseConfig([]string{"-mode", "closed", "-label", "x"})
	require.NoError(t, err)
	require.Equal(t, "closed", c.Mode)
	require.Equal(t, 16, c.Producers)
	require.Equal(t, 1, c.Batch)
}

func TestParseConfigRejectsBadCombinations(t *testing.T) {
	for name, args := range map[string][]string{
		"no mode":           {},
		"unknown mode":      {"-mode", "nope", "-label", "x"},
		"open without rate": {"-mode", "open", "-label", "x", "-workers", "10"},
		"closed no label":   {"-mode", "closed"},
		"fill without jobs": {"-mode", "fill"},
		"batch over 1000":   {"-mode", "closed", "-label", "x", "-batch", "1001"},
		"bad priority":      {"-mode", "fill", "-jobs", "10", "-priority", "5"},
		"nothing to do":     {"-mode", "closed", "-label", "x", "-producers", "0", "-workers", "0"},
	} {
		_, err := parseConfig(args)
		require.Error(t, err, name)
	}
}

func TestWorkerSplitRespectsTheLeaseCap(t *testing.T) {
	require.Equal(t, []int{}, workerSplit(0))
	require.Equal(t, []int{10}, workerSplit(10))
	require.Equal(t, []int{50}, workerSplit(50))
	require.Equal(t, []int{50, 50, 50, 50}, workerSplit(200))
	require.Equal(t, []int{50, 20}, workerSplit(70))
}

func TestParseConfigKindAndProbes(t *testing.T) {
	c, err := parseConfig([]string{"-mode", "fill", "-jobs", "10"})
	require.NoError(t, err)
	require.Equal(t, "bench", c.Kind)
	require.True(t, c.Probes)

	c, err = parseConfig([]string{"-mode", "fill", "-jobs", "10", "-kind", "normal", "-probes=false"})
	require.NoError(t, err)
	require.Equal(t, "normal", c.Kind)
	require.False(t, c.Probes)

	_, err = parseConfig([]string{"-mode", "fill", "-jobs", "10", "-kind", ""})
	require.Error(t, err, "an empty kind matches no handler")
}
