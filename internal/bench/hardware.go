package bench

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// Hardware describes the machine for a results file: a number without it
// cannot be compared. CHUNKD_BENCH_HW overrides the probe.
func Hardware() string {
	if hw := os.Getenv("CHUNKD_BENCH_HW"); hw != "" {
		return hw
	}
	return fmt.Sprintf("%s; %d logical CPUs; %s; %s %s/%s", cpuModel(), runtime.NumCPU(), ram(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func run(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func cpuModel() string {
	switch runtime.GOOS {
	case "windows":
		if s := run("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"); s != "" {
			return s
		}
	case "linux":
		b, _ := os.ReadFile("/proc/cpuinfo")
		for _, l := range strings.Split(string(b), "\n") {
			if k, v, ok := strings.Cut(l, ":"); ok && strings.TrimSpace(k) == "model name" {
				return strings.TrimSpace(v)
			}
		}
	case "darwin":
		if s := run("sysctl", "-n", "machdep.cpu.brand_string"); s != "" {
			return s
		}
	}
	return "unknown CPU"
}

func ram() string {
	var bytes int64
	switch runtime.GOOS {
	case "windows":
		fmt.Sscan(run("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory"), &bytes)
	case "linux":
		b, _ := os.ReadFile("/proc/meminfo")
		for _, l := range strings.Split(string(b), "\n") {
			if rest, ok := strings.CutPrefix(l, "MemTotal:"); ok {
				var kb int64
				fmt.Sscan(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), &kb)
				bytes = kb << 10
			}
		}
	case "darwin":
		fmt.Sscan(run("sysctl", "-n", "hw.memsize"), &bytes)
	}
	if bytes == 0 {
		return "unknown RAM"
	}
	return fmt.Sprintf("%.0f GiB RAM", float64(bytes)/(1<<30))
}
