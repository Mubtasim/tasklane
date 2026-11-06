package probe

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// ScrapeSum fetches a Prometheus text endpoint and returns SumSamples.
func ScrapeSum(ctx context.Context, url, name string, match map[string]string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return SumSamples(resp.Body, name, match)
}

// SumSamples adds up every sample named exactly name whose labels include all
// of match. It parses only what Tasklane's own /metrics prints (label values
// without commas, quotes or escapes), which keeps the load generator free of
// the Prometheus parsing library and its version churn.
func SumSamples(r io.Reader, name string, match map[string]string) (float64, error) {
	var sum float64
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		series, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		metric, labels, _ := strings.Cut(series, "{")
		if metric != name || !labelsMatch(strings.TrimSuffix(labels, "}"), match) {
			continue
		}
		v, err := strconv.ParseFloat(strings.Fields(value)[0], 64)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", line, err)
		}
		sum += v
	}
	return sum, sc.Err()
}

func labelsMatch(labels string, match map[string]string) bool {
	have := map[string]string{}
	for _, kv := range strings.Split(labels, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if ok {
			have[k] = strings.Trim(v, `"`)
		}
	}
	for k, v := range match {
		if have[k] != v {
			return false
		}
	}
	return true
}
