package scheduler

type AHPWeights struct {
	CPU     float64
	Memory  float64
	Storage float64
	IO      float64
	Cache   float64
}

var RandomIndex = map[int]float64{
	1: 0, 2: 0, 3: 0.58, 4: 0.90, 5: 1.12,
	6: 1.24, 7: 1.32, 8: 1.41, 9: 1.45, 10: 1.49,
}

var WorkloadMatrices = map[string][5][5]float64{
	"general": {
		{1, 2, 3, 3, 2},
		{1.0 / 2, 1, 2, 2, 1},
		{1.0 / 3, 1.0 / 2, 1, 1, 1.0 / 2},
		{1.0 / 3, 1.0 / 2, 1, 1, 1.0 / 2},
		{1.0 / 2, 1, 2, 2, 1},
	},
	"compute_intensive": {
		{1, 3, 5, 5, 2},
		{1.0 / 3, 1, 3, 3, 1},
		{1.0 / 5, 1.0 / 3, 1, 1, 1.0 / 3},
		{1.0 / 5, 1.0 / 3, 1, 1, 1.0 / 3},
		{1.0 / 2, 1, 3, 3, 1},
	},
	"io_intensive": {
		{1, 1, 1.0 / 2, 1.0 / 3, 1},
		{1, 1, 1.0 / 2, 1.0 / 3, 1},
		{2, 2, 1, 1.0 / 2, 2},
		{3, 3, 2, 1, 3},
		{1, 1, 1.0 / 2, 1.0 / 3, 1},
	},
	"memory_intensive": {
		{1, 1.0 / 3, 2, 2, 1},
		{3, 1, 4, 4, 2},
		{1.0 / 2, 1.0 / 4, 1, 1, 1.0 / 2},
		{1.0 / 2, 1.0 / 4, 1, 1, 1.0 / 2},
		{1, 1.0 / 2, 2, 2, 1},
	},
}

func CalculateAHPWeights(matrix [5][5]float64, crThreshold float64) (AHPWeights, float64) {
	const n = 5

	var colSums [n]float64
	for j := 0; j < n; j++ {
		for i := 0; i < n; i++ {
			colSums[j] += matrix[i][j]
		}
	}

	var weights [n]float64
	for i := 0; i < n; i++ {
		var rowSum float64
		for j := 0; j < n; j++ {
			rowSum += matrix[i][j] / colSums[j]
		}
		weights[i] = rowSum / float64(n)
	}

	var lambdaMax float64
	for i := 0; i < n; i++ {
		lambdaMax += weights[i] * colSums[i]
	}

	ci := (lambdaMax - float64(n)) / float64(n-1)

	ri := RandomIndex[n]
	cr := 0.0
	if ri > 0 {
		cr = ci / ri
	}

	if cr >= crThreshold {
		return AHPWeights{
			CPU: 0.2, Memory: 0.2, Storage: 0.2, IO: 0.2, Cache: 0.2,
		}, cr
	}

	return AHPWeights{
		CPU:     weights[0],
		Memory:  weights[1],
		Storage: weights[2],
		IO:      weights[3],
		Cache:   weights[4],
	}, cr
}

func GetPerformanceWeights(workloadType string, crThreshold float64) (AHPWeights, float64) {
	matrix, ok := WorkloadMatrices[workloadType]
	if !ok {
		matrix = WorkloadMatrices["general"]
	}
	return CalculateAHPWeights(matrix, crThreshold)
}
