package store

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestTruncateUTF8(t *testing.T) {
	require.Equal(t, "short", TruncateUTF8("short", 10))
	require.Equal(t, "abc", TruncateUTF8("abcdef", 3))
	// "é" is 2 bytes: cutting at 3 would split the second one.
	require.Equal(t, "é", TruncateUTF8("éé", 3))
	long := strings.Repeat("€", 2000) // 3 bytes each
	got := TruncateUTF8(long, MaxErrorBytes)
	require.LessOrEqual(t, len(got), MaxErrorBytes)
	require.Greater(t, len(got), MaxErrorBytes-3)
	require.True(t, utf8.ValidString(got))
}
