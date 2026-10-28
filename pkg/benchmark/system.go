package benchmark

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
)

type SystemMonitor struct {
	mu      sync.Mutex
	samples SystemSamples
	proc    *process.Process
}

func NewSystemMonitor() *SystemMonitor {
	m := &SystemMonitor{}
	if p, err := process.NewProcess(int32(os.Getpid())); err == nil {
		m.proc = p

		_, _ = p.Percent(0)
	}
	return m
}

func (m *SystemMonitor) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sample()
		}
	}
}

func (m *SystemMonitor) sample() {
	cpuPcts, err := cpu.Percent(0, false)
	cpuPct := 0.0
	if err == nil && len(cpuPcts) > 0 {
		cpuPct = cpuPcts[0]
	}

	memStat, err := mem.VirtualMemory()
	memMB := 0.0
	if err == nil && memStat != nil {
		memMB = float64(memStat.Used) / (1024 * 1024)
	}

	procCPU, procRSS := 0.0, 0.0
	if m.proc != nil {
		if pct, err := m.proc.Percent(0); err == nil {
			procCPU = pct
		}
		if mi, err := m.proc.MemoryInfo(); err == nil && mi != nil {
			procRSS = float64(mi.RSS) / (1024 * 1024)
		}
	}

	m.mu.Lock()
	m.samples.CPUSamples = append(m.samples.CPUSamples, cpuPct)
	m.samples.MemorySamples = append(m.samples.MemorySamples, memMB)
	m.samples.ProcCPUSamples = append(m.samples.ProcCPUSamples, procCPU)
	m.samples.ProcRSSSamples = append(m.samples.ProcRSSSamples, procRSS)
	m.mu.Unlock()
}

func (m *SystemMonitor) Samples() SystemSamples {
	m.mu.Lock()
	defer m.mu.Unlock()
	return SystemSamples{
		CPUSamples:     append([]float64(nil), m.samples.CPUSamples...),
		MemorySamples:  append([]float64(nil), m.samples.MemorySamples...),
		ProcCPUSamples: append([]float64(nil), m.samples.ProcCPUSamples...),
		ProcRSSSamples: append([]float64(nil), m.samples.ProcRSSSamples...),
	}
}
