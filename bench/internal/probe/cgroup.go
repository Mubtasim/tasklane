// Package probe reads the numbers a load-test run is judged by: container
// CPU, server metrics and Postgres statistics.
package probe

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// CgroupCPU returns the total CPU time used by the processes in a cgroup v2
// directory, for example
// /sys/fs/cgroup/system.slice/docker-<container id>.scope. The difference of
// two readings divided by the jobs in between is the CPU per job, which
// unlike throughput carries over to other machines.
func CgroupCPU(dir string) (time.Duration, error) {
	f, err := os.Open(filepath.Join(dir, "cpu.stat"))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), " ")
		if ok && k == "usage_usec" {
			us, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("cpu.stat usage_usec: %w", err)
			}
			return time.Duration(us) * time.Microsecond, nil
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("%s: no usage_usec line", filepath.Join(dir, "cpu.stat"))
}
