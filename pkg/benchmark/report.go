package benchmark

import (
	"encoding/json"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"os"
	"sort"
)

type Report struct {
	Scheduler     string          `json:"scheduler"`
	Language      string          `json:"language"`
	Version       string          `json:"version"`
	BenchmarkType string          `json:"benchmark_type"`
	Config        ReportConfig    `json:"config"`
	Aggregate     AggregateReport `json:"aggregate"`
	PerRun        []PerRunReport  `json:"per_run"`
}

type ReportConfig struct {
	Pods       int    `json:"pods"`
	Nodes      int    `json:"nodes"`
	Clusters   int    `json:"clusters"`
	Runs       int    `json:"runs"`
	Priority   string `json:"priority"`
	TimingMode string `json:"timing_mode"`
	Schedulers int    `json:"scheduler_instances"`

	Scorer       string `json:"scorer"`
	PrivacyGates bool   `json:"privacy_gates"`
	DisableReuse bool   `json:"disable_reuse"`

	ReuseClasses bool `json:"reuse_classes"`
	RecheckGates bool `json:"recheck_gates"`

	KeepCompliant bool `json:"keep_compliant_capacity,omitempty"`

	ClientQPS   float64 `json:"client_qps"`
	ClientBurst int     `json:"client_burst"`

	ArrivalBatch       int     `json:"arrival_batch"`
	ArrivalIntervalMs  int     `json:"arrival_interval_ms"`
	OfferedRatePodsSec float64 `json:"offered_rate_pods_sec"`

	Federated      bool              `json:"federated"`
	ClusterNames   []string          `json:"cluster_names,omitempty"`
	ClusterRegions map[string]string `json:"cluster_regions,omitempty"`

	MovePenalty float64 `json:"move_penalty,omitempty"`

	UtilizationLimit float64 `json:"utilization_limit"`
}

type AggregateReport struct {
	ThroughputPodsPerSec StatSummary `json:"throughput_pods_sec"`
	P50LatencyMs         StatSummary `json:"p50_latency_ms"`
	P95LatencyMs         StatSummary `json:"p95_latency_ms"`
	P99LatencyMs         StatSummary `json:"p99_latency_ms"`
	P999LatencyMs        StatSummary `json:"p999_latency_ms"`
	QueueReuseRate       StatSummary `json:"queue_reuse_rate"`
	FilterPassRate       StatSummary `json:"filter_pass_rate"`
	ScheduledCount       StatSummary `json:"scheduled_count"`
	FailedCount          StatSummary `json:"failed_count"`
	PeakCPUPct           StatSummary `json:"peak_cpu_pct"`
	PeakMemoryMB         StatSummary `json:"peak_memory_mb"`
	AvgCPUPct            StatSummary `json:"avg_cpu_pct"`
	AvgMemoryMB          StatSummary `json:"avg_memory_mb"`
	ProcPeakCPUPct       StatSummary `json:"proc_peak_cpu_pct"`
	ProcAvgCPUPct        StatSummary `json:"proc_avg_cpu_pct"`
	ProcPeakRSSMB        StatSummary `json:"proc_peak_rss_mb"`
	ProcAvgRSSMB         StatSummary `json:"proc_avg_rss_mb"`
	EncMeanLatMs         StatSummary `json:"enc_mean_latency_ms"`
	NonEncMeanLatMs      StatSummary `json:"non_enc_mean_latency_ms"`
	EncP99LatMs          StatSummary `json:"enc_p99_latency_ms"`
	NonEncP99LatMs       StatSummary `json:"non_enc_p99_latency_ms"`
	MigrationRate        StatSummary `json:"migration_rate"`
	MigrationCount       StatSummary `json:"migration_count"`
	LagMs                StatSummary `json:"last_create_to_last_bind_ms"`
}

type PerRunReport struct {
	Run                  int                         `json:"run"`
	ThroughputPodsPerSec float64                     `json:"throughput_pods_sec"`
	DurationMs           float64                     `json:"duration_ms"`
	Scheduled            int                         `json:"scheduled"`
	Failed               int                         `json:"failed"`
	QueueReuseCount      int                         `json:"queue_reuse_count"`
	QueueReuseRate       float64                     `json:"queue_reuse_rate"`
	MigrationCount       int                         `json:"migration_count"`
	MigrationRate        float64                     `json:"migration_rate"`
	LagMs                float64                     `json:"last_create_to_last_bind_ms"`
	MigrationDetails     *MigrationSummary           `json:"migration_details,omitempty"`
	FilterPassRate       float64                     `json:"filter_pass_rate"`
	LatencyPercentiles   LatencyPercentiles          `json:"latency_percentiles_ms"`
	FilterPercentiles    FilterRankPercentiles       `json:"filter_percentiles_ms"`
	RankPercentiles      FilterRankPercentiles       `json:"rank_percentiles_ms"`
	BindPercentiles      FilterRankPercentiles       `json:"bind_percentiles_ms"`
	System               SystemReport                `json:"system"`
	EncryptionOverhead   EncryptionOverhead          `json:"encryption_overhead"`
	SuccessRate          float64                     `json:"success_rate"`
	BindConflicts        int                         `json:"bind_conflicts"`
	BindOtherErrors      int                         `json:"bind_other_errors"`
	ReserveRejects       int                         `json:"reserve_rejects"`
	GateRejects          int                         `json:"gate_rejects"`
	ChangedDuringBind    int                         `json:"changed_during_bind"`
	AuditMode            string                      `json:"audit_mode,omitempty"`
	AuditEntries         uint64                      `json:"audit_entries,omitempty"`
	AuditBytes           int64                       `json:"audit_bytes,omitempty"`
	AuditErrors          int                         `json:"audit_errors,omitempty"`
	AuditFile            string                      `json:"audit_file,omitempty"`
	LabelChanges         int                         `json:"label_changes"`
	LabelPatchErrors     int                         `json:"label_patch_errors"`
	ReuseFallbacks       int                         `json:"reuse_fallbacks"`
	HomeCopiesLeft       int                         `json:"home_copies_left"`
	StaleRankings        int                         `json:"stale_rankings"`
	Placement            *scheduler.PlacementQuality `json:"placement,omitempty"`
	Violations           *scheduler.ViolationReport  `json:"violations,omitempty"`
}

type EncryptionOverhead struct {
	EncPods         int     `json:"enc_pods"`
	NonEncPods      int     `json:"non_enc_pods"`
	EncMeanLatMs    float64 `json:"enc_mean_latency_ms"`
	NonEncMeanLatMs float64 `json:"non_enc_mean_latency_ms"`
	EncP99LatMs     float64 `json:"enc_p99_latency_ms"`
	NonEncP99LatMs  float64 `json:"non_enc_p99_latency_ms"`
}

type MigrationSummary struct {
	TotalMigrations   int            `json:"total_migrations"`
	MigrationRate     float64        `json:"migration_rate_pct"`
	MeanMigrationMs   float64        `json:"mean_migration_latency_ms"`
	P50MigrationMs    float64        `json:"p50_migration_latency_ms"`
	P95MigrationMs    float64        `json:"p95_migration_latency_ms"`
	P99MigrationMs    float64        `json:"p99_migration_latency_ms"`
	MeanLocalLatMs    float64        `json:"mean_local_latency_ms"`
	MeanMigratedLatMs float64        `json:"mean_migrated_latency_ms"`
	ClusterFlows      map[string]int `json:"cluster_flows"`

	PerHomeCluster map[string]HomeClusterStats `json:"per_home_cluster,omitempty"`
}

type HomeClusterStats struct {
	Pods              int     `json:"pods"`
	Migrated          int     `json:"migrated"`
	ResidencyPods     int     `json:"residency_pods"`
	ResidencyMigrated int     `json:"residency_migrated"`
	MeanLatMs         float64 `json:"mean_latency_ms"`
	P50LatMs          float64 `json:"p50_latency_ms"`
	P99LatMs          float64 `json:"p99_latency_ms"`
}

type SystemReport struct {
	PeakCPUPct     float64   `json:"peak_cpu_pct"`
	PeakMemoryMB   float64   `json:"peak_memory_mb"`
	AvgCPUPct      float64   `json:"avg_cpu_pct"`
	AvgMemoryMB    float64   `json:"avg_memory_mb"`
	ProcPeakCPUPct float64   `json:"proc_peak_cpu_pct"`
	ProcAvgCPUPct  float64   `json:"proc_avg_cpu_pct"`
	ProcPeakRSSMB  float64   `json:"proc_peak_rss_mb"`
	ProcAvgRSSMB   float64   `json:"proc_avg_rss_mb"`
	CPUSamples     []float64 `json:"cpu_samples"`
	MemorySamples  []float64 `json:"memory_samples"`
	ProcCPUSamples []float64 `json:"proc_cpu_samples"`
	ProcRSSSamples []float64 `json:"proc_rss_samples"`
}

func BuildReport(cfg ReportConfig, runs []RunMetrics) Report {
	cfg.UtilizationLimit = scheduler.UtilizationLimit
	nRuns := len(runs)

	throughputs := make([]float64, nRuns)
	p50s := make([]float64, nRuns)
	p95s := make([]float64, nRuns)
	p99s := make([]float64, nRuns)
	p999s := make([]float64, nRuns)
	queueReuseRates := make([]float64, nRuns)
	filterPassRates := make([]float64, nRuns)
	scheduledCounts := make([]float64, nRuns)
	failedCounts := make([]float64, nRuns)
	peakCPUs := make([]float64, nRuns)
	peakMems := make([]float64, nRuns)
	avgCPUs := make([]float64, nRuns)
	avgMems := make([]float64, nRuns)
	procPeakCPUs := make([]float64, nRuns)
	procAvgCPUs := make([]float64, nRuns)
	procPeakRSS := make([]float64, nRuns)
	procAvgRSS := make([]float64, nRuns)
	encMeanLats := make([]float64, nRuns)
	nonEncMeanLats := make([]float64, nRuns)
	encP99Lats := make([]float64, nRuns)
	nonEncP99Lats := make([]float64, nRuns)
	migrationRates := make([]float64, nRuns)
	migrationCounts := make([]float64, nRuns)
	lags := make([]float64, nRuns)

	perRunReports := make([]PerRunReport, nRuns)

	for i, rm := range runs {
		boundTotal, boundFilter, boundRank, boundBind := rm.BoundLatenciesNs()
		_ = boundFilter
		_ = boundRank
		_ = boundBind
		totalNs := make([]int64, len(rm.PodLatencies))
		filterNs := make([]int64, len(rm.PodLatencies))
		rankNs := make([]int64, len(rm.PodLatencies))
		bindNs := make([]int64, len(rm.PodLatencies))
		for j, pl := range rm.PodLatencies {
			totalNs[j] = pl.TotalNs
			filterNs[j] = pl.FilterNs
			rankNs[j] = pl.RankNs
			bindNs[j] = pl.BindNs
		}

		latPct := ComputeLatencyPercentiles(boundTotal)
		filterPct := ComputeFilterRankPercentiles(filterNs)
		rankPct := ComputeFilterRankPercentiles(rankNs)
		bindPct := ComputeFilterRankPercentiles(bindNs)

		tp := rm.ThroughputPodsPerSec()
		qrr := rm.QueueReuseRate()
		fpr := rm.FilterPassRate()

		throughputs[i] = tp
		p50s[i] = latPct.P50
		p95s[i] = latPct.P95
		p99s[i] = latPct.P99
		p999s[i] = latPct.P999
		queueReuseRates[i] = qrr
		filterPassRates[i] = fpr
		scheduledCounts[i] = float64(rm.Scheduled)
		failedCounts[i] = float64(rm.Failed)
		peakCPUs[i] = rm.SystemStats.PeakCPU()
		peakMems[i] = rm.SystemStats.PeakMemory()
		avgCPUs[i] = rm.SystemStats.AvgCPU()
		avgMems[i] = rm.SystemStats.AvgMemory()
		procPeakCPUs[i] = rm.SystemStats.PeakProcCPU()
		procAvgCPUs[i] = rm.SystemStats.AvgProcCPU()
		procPeakRSS[i] = rm.SystemStats.PeakProcRSS()
		procAvgRSS[i] = rm.SystemStats.AvgProcRSS()

		encNs, nonEncNs := rm.EncryptionLatencies()
		encOverhead := computeEncryptionOverhead(encNs, nonEncNs)
		encMeanLats[i] = encOverhead.EncMeanLatMs
		nonEncMeanLats[i] = encOverhead.NonEncMeanLatMs
		encP99Lats[i] = encOverhead.EncP99LatMs
		nonEncP99Lats[i] = encOverhead.NonEncP99LatMs
		migrationRates[i] = rm.MigrationRate()
		migrationCounts[i] = float64(rm.MigrationCount)
		lags[i] = rm.LagMs()

		perRunReports[i] = PerRunReport{
			Run:                  i + 1,
			ThroughputPodsPerSec: tp,
			DurationMs:           rm.DurationMs(),
			Scheduled:            rm.Scheduled,
			Failed:               rm.Failed,
			QueueReuseCount:      rm.QueueReuseCount,
			QueueReuseRate:       qrr,
			MigrationCount:       rm.MigrationCount,
			MigrationRate:        rm.MigrationRate(),
			LagMs:                rm.LagMs(),
			FilterPassRate:       fpr,
			LatencyPercentiles:   latPct,
			FilterPercentiles:    filterPct,
			RankPercentiles:      rankPct,
			BindPercentiles:      bindPct,
			SuccessRate:          rm.SuccessRate(),
			BindConflicts:        rm.BindConflicts,
			BindOtherErrors:      rm.BindOtherErrors,
			ReserveRejects:       rm.ReserveRejects,
			GateRejects:          rm.GateRejects,
			ChangedDuringBind:    rm.ChangedDuringBind,
			AuditMode:            rm.AuditMode,
			AuditEntries:         rm.AuditEntries,
			AuditBytes:           rm.AuditBytes,
			AuditErrors:          rm.AuditErrors,
			AuditFile:            rm.AuditFile,
			LabelChanges:         rm.LabelChanges,
			LabelPatchErrors:     rm.LabelPatchErrors,
			ReuseFallbacks:       rm.ReuseFallbacks,
			HomeCopiesLeft:       rm.HomeCopiesLeft,
			StaleRankings:        rm.StaleRankings,
			Placement:            rm.Placement,
			Violations:           rm.Violations,
			System: SystemReport{
				PeakCPUPct:     rm.SystemStats.PeakCPU(),
				PeakMemoryMB:   rm.SystemStats.PeakMemory(),
				AvgCPUPct:      rm.SystemStats.AvgCPU(),
				AvgMemoryMB:    rm.SystemStats.AvgMemory(),
				ProcPeakCPUPct: rm.SystemStats.PeakProcCPU(),
				ProcAvgCPUPct:  rm.SystemStats.AvgProcCPU(),
				ProcPeakRSSMB:  rm.SystemStats.PeakProcRSS(),
				ProcAvgRSSMB:   rm.SystemStats.AvgProcRSS(),
				CPUSamples:     rm.SystemStats.CPUSamples,
				MemorySamples:  rm.SystemStats.MemorySamples,
				ProcCPUSamples: rm.SystemStats.ProcCPUSamples,
				ProcRSSSamples: rm.SystemStats.ProcRSSSamples,
			},
			EncryptionOverhead: encOverhead,
		}
		if ms, ok := rm.MigrationSummary.(*MigrationSummary); ok && ms != nil {
			perRunReports[i].MigrationDetails = ms
		}
	}

	return Report{
		Scheduler:     "gated-scheduler",
		Language:      "go",
		Version:       "1.0.0",
		BenchmarkType: "kwok",
		Config:        cfg,
		Aggregate: AggregateReport{
			ThroughputPodsPerSec: ComputeStats(throughputs),
			P50LatencyMs:         ComputeStats(p50s),
			P95LatencyMs:         ComputeStats(p95s),
			P99LatencyMs:         ComputeStats(p99s),
			P999LatencyMs:        ComputeStats(p999s),
			QueueReuseRate:       ComputeStats(queueReuseRates),
			FilterPassRate:       ComputeStats(filterPassRates),
			ScheduledCount:       ComputeStats(scheduledCounts),
			FailedCount:          ComputeStats(failedCounts),
			PeakCPUPct:           ComputeStats(peakCPUs),
			PeakMemoryMB:         ComputeStats(peakMems),
			AvgCPUPct:            ComputeStats(avgCPUs),
			AvgMemoryMB:          ComputeStats(avgMems),
			ProcPeakCPUPct:       ComputeStats(procPeakCPUs),
			ProcAvgCPUPct:        ComputeStats(procAvgCPUs),
			ProcPeakRSSMB:        ComputeStats(procPeakRSS),
			ProcAvgRSSMB:         ComputeStats(procAvgRSS),
			EncMeanLatMs:         ComputeStats(encMeanLats),
			NonEncMeanLatMs:      ComputeStats(nonEncMeanLats),
			EncP99LatMs:          ComputeStats(encP99Lats),
			NonEncP99LatMs:       ComputeStats(nonEncP99Lats),
			MigrationRate:        ComputeStats(migrationRates),
			MigrationCount:       ComputeStats(migrationCounts),
			LagMs:                ComputeStats(lags),
		},
		PerRun: perRunReports,
	}
}

func computeEncryptionOverhead(encNs, nonEncNs []int64) EncryptionOverhead {
	eo := EncryptionOverhead{
		EncPods:    len(encNs),
		NonEncPods: len(nonEncNs),
	}

	if len(encNs) > 0 {
		var sum float64
		msSlice := make([]float64, len(encNs))
		for i, v := range encNs {
			ms := float64(v) / 1e6
			msSlice[i] = ms
			sum += ms
		}
		eo.EncMeanLatMs = sum / float64(len(encNs))
		sort.Float64s(msSlice)
		eo.EncP99LatMs = Percentile(msSlice, 99)
	}

	if len(nonEncNs) > 0 {
		var sum float64
		msSlice := make([]float64, len(nonEncNs))
		for i, v := range nonEncNs {
			ms := float64(v) / 1e6
			msSlice[i] = ms
			sum += ms
		}
		eo.NonEncMeanLatMs = sum / float64(len(nonEncNs))
		sort.Float64s(msSlice)
		eo.NonEncP99LatMs = Percentile(msSlice, 99)
	}

	return eo
}

func WriteJSON(path string, report Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
