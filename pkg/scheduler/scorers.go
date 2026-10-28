package scheduler

import (
	"fmt"
	"math"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

type Scorer string

const (
	ScorerAHP Scorer = "ahp"

	ScorerEqual Scorer = "equal"

	ScorerK8s Scorer = "k8s"

	ScorerAdversarial Scorer = "adversarial"
)

func ParseScorer(s string) (Scorer, error) {
	switch Scorer(s) {
	case "", ScorerAHP:
		return ScorerAHP, nil
	case ScorerEqual, ScorerK8s, ScorerAdversarial:
		return Scorer(s), nil
	}
	return "", fmt.Errorf("unknown scorer %q: want ahp, equal, k8s or adversarial", s)
}

var EqualAHPWeights = AHPWeights{CPU: 0.2, Memory: 0.2, Storage: 0.2, IO: 0.2, Cache: 0.2}

const maxNodeScore = 100

func k8sScore(node *types.Node, pod *types.Pod, usage *NodeResourceTracker) float64 {
	cpuCap, memCap := int64(node.CPUCores)*1000, int64(node.Memory)*1024
	var cpuUsed, memUsed int64
	if usage != nil {
		cpuUsed, memUsed, _ = usage.Get(node.Name)
	}
	var cpuReq, memReq int64
	if pod != nil {
		cpuReq, memReq = int64(pod.CPU), int64(pod.Memory)
	}
	requested := []int64{cpuUsed + cpuReq, memUsed + memReq}
	allocatable := []int64{cpuCap, memCap}
	return float64(leastAllocatedScore(requested, allocatable) + balancedAllocationScore(requested, allocatable))
}

func leastAllocatedScore(requested, allocatable []int64) int64 {
	var nodeScore, weightSum int64
	for i := range requested {
		if allocatable[i] == 0 {
			continue
		}
		var s int64
		if requested[i] <= allocatable[i] {
			s = (allocatable[i] - requested[i]) * maxNodeScore / allocatable[i]
		}
		nodeScore += s
		weightSum++
	}
	if weightSum == 0 {
		return 0
	}
	return nodeScore / weightSum
}

func balancedAllocationScore(requested, allocatable []int64) int64 {
	var fractions []float64
	var total float64
	for i := range requested {
		if allocatable[i] == 0 {
			continue
		}
		f := float64(requested[i]) / float64(allocatable[i])
		if f > 1 {
			f = 1
		}
		total += f
		fractions = append(fractions, f)
	}
	std := 0.0
	switch {
	case len(fractions) == 2:
		std = math.Abs((fractions[0] - fractions[1]) / 2)
	case len(fractions) > 2:
		mean := total / float64(len(fractions))
		var sum float64
		for _, f := range fractions {
			sum += (f - mean) * (f - mean)
		}
		std = math.Sqrt(sum / float64(len(fractions)))
	}
	return int64((1 - std) * maxNodeScore)
}

func adversarialScore(node *types.Node, pod *types.Pod, ahp float64) float64 {
	broken := 0
	if pod != nil {
		broken = len(CheckViolations(*pod, *node))
	}
	return 1000*float64(broken) + ahp
}
