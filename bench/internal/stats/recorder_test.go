package stats

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecorderPercentiles(t *testing.T) {
	r := NewRecorder()
	for i := 1; i <= 1000; i++ {
		r.Record(time.Duration(i) * time.Millisecond)
	}
	s := r.Summary()
	require.Equal(t, int64(1000), s.Count)
	require.InDelta(t, 500, s.P50.Milliseconds(), 5)
	require.InDelta(t, 990, s.P99.Milliseconds(), 10)
	require.InDelta(t, 999, s.P999.Milliseconds(), 10)
	require.InDelta(t, 1000, s.Max.Milliseconds(), 10)
}

func TestRecorderClampsOutOfRangeValues(t *testing.T) {
	r := NewRecorder()
	r.Record(-time.Second) // clock oddity: counted as the minimum, not dropped
	r.Record(time.Hour)    // beyond the 10 min range: counted as the maximum
	require.Equal(t, int64(2), r.Summary().Count)
}

func TestRecorderIsSafeForConcurrentUse(t *testing.T) {
	r := NewRecorder()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				r.Record(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(8000), r.Summary().Count)
}

func TestEmptyRecorderSummaryIsZero(t *testing.T) {
	require.Equal(t, Summary{}, NewRecorder().Summary())
}
