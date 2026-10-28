package scheduler

import (
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func TestShadowRaces(t *testing.T) {
	eu := types.Node{Name: "eu", CPUCores: 4, Memory: 64, Storage: 100, Bandwidth: 1000, IOSpeed: 1000, Region: types.RegionEurope}
	eu2 := types.Node{Name: "eu2", CPUCores: 4, Memory: 64, Storage: 100, Bandwidth: 1000, IOSpeed: 1000, Region: types.RegionEurope}
	us := types.Node{Name: "us", CPUCores: 4, Memory: 64, Storage: 100, Bandwidth: 1000, IOSpeed: 1000, Region: types.RegionUS}
	nodes := []types.Node{eu, eu2, us}
	pod := types.Pod{CPU: 2000, Memory: 1024, Latency: 100, GDPRRequired: true}
	w := AHPWeights{CPU: 0.2, Memory: 0.2, Storage: 0.2, IO: 0.2, Cache: 0.2}

	tr := NewNodeResourceTracker()
	tr.Update("eu", types.Pod{CPU: 4000})
	sr, ok := ShadowEvaluate(nodes, pod, tr, w, "balanced", RankContext{}, "eu")
	if !ok || !sr.Raced || sr.Misplaced || sr.NotPassing {
		t.Fatalf("a full but otherwise eligible node is a race: %+v (ok %v)", sr, ok)
	}

	sr, ok = ShadowEvaluate(nodes, pod, NewNodeResourceTracker(), w, "balanced", RankContext{}, "us")
	if !ok || sr.Raced || !sr.NotPassing || !sr.Misplaced {
		t.Fatalf("a node outside Europe for a residency-bound pod is a misplacement: %+v (ok %v)", sr, ok)
	}

	sr, ok = ShadowEvaluate(nodes, pod, NewNodeResourceTracker(), w, "balanced", RankContext{}, "eu")
	if !ok || sr.Raced || sr.NotPassing {
		t.Fatalf("an eligible node with room is neither: %+v (ok %v)", sr, ok)
	}
}
