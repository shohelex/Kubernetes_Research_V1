package benchmark

import (
	"math"
	"sort"
)

type StatSummary struct {
	Mean   float64 `json:"mean"`
	Std    float64 `json:"std"`
	CI95Lo float64 `json:"ci95_lo"`
	CI95Hi float64 `json:"ci95_hi"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	CVPct  float64 `json:"cv_pct"`
}

type LatencyPercentiles struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
}

type FilterRankPercentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

var tValues = map[int]float64{
	1:   12.706,
	2:   4.303,
	3:   3.182,
	4:   2.776,
	5:   2.571,
	6:   2.447,
	7:   2.365,
	8:   2.306,
	9:   2.262,
	10:  2.228,
	11:  2.201,
	12:  2.179,
	13:  2.160,
	14:  2.145,
	15:  2.131,
	16:  2.120,
	17:  2.110,
	18:  2.101,
	19:  2.093,
	20:  2.086,
	25:  2.060,
	30:  2.042,
	40:  2.021,
	50:  2.009,
	60:  2.000,
	80:  1.990,
	100: 1.984,
	120: 1.980,
}

func getTValue(df int) float64 {
	if v, ok := tValues[df]; ok {
		return v
	}
	if df <= 0 {
		return 1.96
	}

	bestDF := 0
	for k := range tValues {
		if k <= df && k > bestDF {
			bestDF = k
		}
	}
	if bestDF > 0 {
		return tValues[bestDF]
	}
	return 1.96
}

func ComputeStats(values []float64) StatSummary {
	n := len(values)
	if n == 0 {
		return StatSummary{}
	}

	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(n)

	minVal := values[0]
	maxVal := values[0]
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	if n == 1 {
		return StatSummary{
			Mean:   mean,
			Std:    0,
			CI95Lo: mean,
			CI95Hi: mean,
			Min:    minVal,
			Max:    maxVal,
			CVPct:  0,
		}
	}

	var sumSq float64
	for _, v := range values {
		d := v - mean
		sumSq += d * d
	}
	std := math.Sqrt(sumSq / float64(n-1))

	sem := std / math.Sqrt(float64(n))

	t := getTValue(n - 1)
	ci95Lo := mean - t*sem
	ci95Hi := mean + t*sem

	cvPct := 0.0
	if mean != 0 {
		cvPct = (std / math.Abs(mean)) * 100
	}

	return StatSummary{
		Mean:   mean,
		Std:    std,
		CI95Lo: ci95Lo,
		CI95Hi: ci95Hi,
		Min:    minVal,
		Max:    maxVal,
		CVPct:  cvPct,
	}
}

func Percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}

	idx := (p / 100.0) * float64(n-1)
	lower := int(math.Floor(idx))
	upper := lower + 1
	if upper >= n {
		return sorted[n-1]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

func ComputeLatencyPercentiles(latenciesNs []int64) LatencyPercentiles {
	if len(latenciesNs) == 0 {
		return LatencyPercentiles{}
	}

	ms := make([]float64, len(latenciesNs))
	for i, v := range latenciesNs {
		ms[i] = float64(v) / 1e6
	}
	sort.Float64s(ms)

	return LatencyPercentiles{
		P50:  Percentile(ms, 50),
		P90:  Percentile(ms, 90),
		P95:  Percentile(ms, 95),
		P99:  Percentile(ms, 99),
		P999: Percentile(ms, 99.9),
		Max:  ms[len(ms)-1],
	}
}

func ComputeFilterRankPercentiles(latenciesNs []int64) FilterRankPercentiles {
	if len(latenciesNs) == 0 {
		return FilterRankPercentiles{}
	}

	ms := make([]float64, len(latenciesNs))
	for i, v := range latenciesNs {
		ms[i] = float64(v) / 1e6
	}
	sort.Float64s(ms)

	return FilterRankPercentiles{
		P50: Percentile(ms, 50),
		P95: Percentile(ms, 95),
		P99: Percentile(ms, 99),
	}
}
