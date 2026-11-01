package metrics

import (
	"maps"

	dto "github.com/prometheus/client_model/go"
)

// Value returns the current value of the series with these labels (name,
// value pairs): a counter's or gauge's value, or a histogram's sample count.
// It returns 0 when the series doesn't exist or m is nil. It is meant for
// tests.
func (m *Metrics) Value(name string, labels ...string) float64 {
	s := m.find(name, labels)
	switch {
	case s == nil:
		return 0
	case s.Counter != nil:
		return s.Counter.GetValue()
	case s.Gauge != nil:
		return s.Gauge.GetValue()
	case s.Histogram != nil:
		return float64(s.Histogram.GetSampleCount())
	}
	return 0
}

func (m *Metrics) has(name string, labels ...string) bool { return m.find(name, labels) != nil }

func (m *Metrics) find(name string, labels []string) *dto.Metric {
	if m == nil {
		return nil
	}
	families, err := m.reg.Gather()
	if err != nil {
		return nil
	}
	want := map[string]string{}
	for i := 0; i+1 < len(labels); i += 2 {
		want[labels[i]] = labels[i+1]
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, s := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range s.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if maps.Equal(got, want) {
				return s
			}
		}
	}
	return nil
}
