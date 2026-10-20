package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKindsAddUpTo100(t *testing.T) {
	require.Equal(t, 100, totalWeight(kinds))
}

func TestPickFollowsTheWeights(t *testing.T) {
	cases := map[int]string{0: "normal", 84: "normal", 85: "flaky", 96: "flaky", 97: "poison", 99: "poison"}
	for roll, want := range cases {
		require.Equal(t, want, pick(kinds, roll), "roll %d", roll)
	}
}
