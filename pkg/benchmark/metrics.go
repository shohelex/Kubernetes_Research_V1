package benchmark

import (
	"time"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
)

type PodLatency struct {
	TotalNs            int64
	FilterNs           int64
	RankNs             int64
	BindNs             int64
	EncryptionRequired bool
	ResidencyRequired  bool
	Migrated           bool
	SourceCluster      string
	TargetCluster      string
	MigrationNs        int64
	Bound              bool
}

type RunMetrics struct {
	RunIndex  int
	StartTime time.Time
	EndTime   time.Time

	SubmissionEnd    time.Time
	PodLatencies     []PodLatency
	Scheduled        int
	Failed           int
	QueueReuseCount  int
	MigrationCount   int
	TotalPods        int
	TotalNodes       int
	Schedulers       int
	FilterPassCounts []int
	SystemStats      SystemSamples
	MigrationSummary interface{}

	BindConflicts   int
	BindOtherErrors int

	ReserveRejects int

	GateRejects int

	ChangedDuringBind int

	AuditMode    string
	AuditEntries uint64
	AuditBytes   int64
	AuditErrors  int
	AuditFile    string

	LabelChanges     int
	LabelPatchErrors int

	ReuseFallbacks int

	HomeCopiesLeft int

	StaleRankings int

	Placement *scheduler.PlacementQuality

	Violations *scheduler.ViolationReport
}

func (r *RunMetrics) MigrationRate() float64 {
	if r.Scheduled <= 0 {
		return 0
	}
	return float64(r.MigrationCount) / float64(r.Scheduled)
}

func (r *RunMetrics) DurationMs() float64 {
	return float64(r.EndTime.Sub(r.StartTime).Nanoseconds()) / 1e6
}

func (r *RunMetrics) LagMs() float64 {
	if r.SubmissionEnd.IsZero() || r.EndTime.Before(r.SubmissionEnd) {
		return 0
	}
	return float64(r.EndTime.Sub(r.SubmissionEnd).Nanoseconds()) / 1e6
}

func (r *RunMetrics) ThroughputPodsPerSec() float64 {
	dur := r.EndTime.Sub(r.StartTime).Seconds()
	if dur <= 0 {
		return 0
	}
	return float64(r.Scheduled) / dur
}

func (r *RunMetrics) BoundLatenciesNs() (total, filter, rank, bind []int64) {
	for _, pl := range r.PodLatencies {
		if !pl.Bound {
			continue
		}
		total = append(total, pl.TotalNs)
		filter = append(filter, pl.FilterNs)
		rank = append(rank, pl.RankNs)
		bind = append(bind, pl.BindNs)
	}
	return
}

func (r *RunMetrics) SuccessRate() float64 {
	if r.TotalPods <= 0 {
		return 0
	}
	return float64(r.Scheduled) / float64(r.TotalPods)
}

func (r *RunMetrics) QueueReuseRate() float64 {
	schedulers := r.Schedulers
	if schedulers <= 0 {
		schedulers = 1
	}
	eligible := r.TotalPods - schedulers
	if eligible <= 0 {
		return 0
	}
	return float64(r.QueueReuseCount) / float64(eligible)
}

func (r *RunMetrics) FilterPassRate() float64 {
	filteredPods := len(r.FilterPassCounts)
	if filteredPods == 0 || r.TotalNodes == 0 {
		return 0
	}
	total := 0
	for _, c := range r.FilterPassCounts {
		total += c
	}
	return float64(total) / float64(filteredPods*r.TotalNodes)
}

func (rm *RunMetrics) EncryptionLatencies() (encNs []int64, nonEncNs []int64) {
	for _, pl := range rm.PodLatencies {
		if pl.TotalNs > 0 {
			if pl.EncryptionRequired {
				encNs = append(encNs, pl.TotalNs)
			} else {
				nonEncNs = append(nonEncNs, pl.TotalNs)
			}
		}
	}
	return
}

type SystemSamples struct {
	CPUSamples     []float64
	MemorySamples  []float64
	ProcCPUSamples []float64
	ProcRSSSamples []float64
}

func peakOf(vals []float64) float64 {
	var peak float64
	for _, v := range vals {
		if v > peak {
			peak = v
		}
	}
	return peak
}

func avgOf(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func (s *SystemSamples) PeakProcCPU() float64 { return peakOf(s.ProcCPUSamples) }

func (s *SystemSamples) AvgProcCPU() float64 { return avgOf(s.ProcCPUSamples) }

func (s *SystemSamples) PeakProcRSS() float64 { return peakOf(s.ProcRSSSamples) }

func (s *SystemSamples) AvgProcRSS() float64 { return avgOf(s.ProcRSSSamples) }

func (s *SystemSamples) PeakCPU() float64 {
	var peak float64
	for _, v := range s.CPUSamples {
		if v > peak {
			peak = v
		}
	}
	return peak
}

func (s *SystemSamples) PeakMemory() float64 {
	var peak float64
	for _, v := range s.MemorySamples {
		if v > peak {
			peak = v
		}
	}
	return peak
}

func (s *SystemSamples) AvgCPU() float64 {
	if len(s.CPUSamples) == 0 {
		return 0
	}
	var sum float64
	for _, v := range s.CPUSamples {
		sum += v
	}
	return sum / float64(len(s.CPUSamples))
}

func (s *SystemSamples) AvgMemory() float64 {
	if len(s.MemorySamples) == 0 {
		return 0
	}
	var sum float64
	for _, v := range s.MemorySamples {
		sum += v
	}
	return sum / float64(len(s.MemorySamples))
}
