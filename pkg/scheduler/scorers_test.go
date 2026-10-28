package scheduler

import (
	"math/rand"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func TestK8sScoreFormulas(t *testing.T) {
	node := types.Node{Name: "n", CPUCores: 10, Memory: 10}
	cases := []struct {
		name             string
		cpuUsed, memUsed uint32
		pod              types.Pod
		want             float64
	}{
		{"balanced", 2000, 2048, types.Pod{CPU: 1000, Memory: 1024}, 170},

		{"unbalanced", 5000, 0, types.Pod{CPU: 1000, Memory: 1024}, 140},

		{"empty", 0, 0, types.Pod{}, 200},

		{"overcommitted", 9000, 0, types.Pod{CPU: 2000}, 100},
	}
	for _, c := range cases {
		tr := NewNodeResourceTracker()
		if c.cpuUsed > 0 || c.memUsed > 0 {
			tr.Update("n", types.Pod{CPU: c.cpuUsed, Memory: c.memUsed})
		}
		pod := c.pod
		if got := k8sScore(&node, &pod, tr); got != c.want {
			t.Errorf("%s: score %v, want %v", c.name, got, c.want)
		}
	}
}

func TestScoreDynamics(t *testing.T) {
	nodes := []types.Node{
		{Name: "a", CPUCores: 16, Memory: 32, Storage: 100, IOSpeed: 100, LCache: 16, Region: types.RegionEurope},
		{Name: "b", CPUCores: 16, Memory: 32, Storage: 100, IOSpeed: 100, LCache: 16, Region: types.RegionEurope},
	}
	pod := types.Pod{CPU: 4000, Memory: 4096}
	w := AHPWeights{CPU: 0.35, Memory: 0.25, Storage: 0.15, IO: 0.15, Cache: 0.10}
	tr := NewNodeResourceTracker()
	rank := func(s Scorer) []RankedNode {
		return RankNodesFor(nodes, w, "balanced", RankContext{Scorer: s, Pod: &pod, Usage: tr})
	}
	ahpBefore, k8sBefore := rank(ScorerAHP), rank(ScorerK8s)
	tr.Update("a", pod)
	ahpAfter, k8sAfter := rank(ScorerAHP), rank(ScorerK8s)
	if ahpBefore[0].Score != ahpAfter[0].Score || ahpBefore[1].Score != ahpAfter[1].Score {
		t.Fatal("a booking changed an AHP score")
	}
	if k8sBefore[0].Score != k8sBefore[1].Score {
		t.Fatal("two identical empty nodes must score the same under k8s")
	}
	if k8sAfter[0].Node.Name != "b" || k8sAfter[0].Score <= k8sAfter[1].Score {
		t.Fatalf("after booking a, the k8s scorer must prefer b: %+v", k8sAfter)
	}
}

func TestAdversarialScorer(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	nodes := randomInventory(rng, 800)
	w := AHPWeights{CPU: 0.35, Memory: 0.25, Storage: 0.15, IO: 0.15, Cache: 0.10}
	saved := PrivacyFiltersEnabled
	defer func() { PrivacyFiltersEnabled = saved }()
	for p := 0; p < 200; p++ {
		pod := randomPod(rng)
		pod.GDPRRequired = true
		pod.Bandwidth, pod.IOSpeed, pod.Latency = 0, 0, 1000
		tr := NewNodeResourceTracker()
		rc := RankContext{Scorer: ScorerAdversarial, Pod: &pod, Usage: tr}

		PrivacyFiltersEnabled = true
		idx := FilterIndices(nodes, pod, tr, nil)
		for _, r := range RankIndexed(nodes, idx, w, "balanced", rc) {
			if v := CheckViolations(pod, r.Node); len(v) > 0 {
				t.Fatalf("pod %d: gates on, the adversarial ranking offers %s, which breaks %v", p, r.Node.Name, v)
			}
		}

		PrivacyFiltersEnabled = false
		idx = FilterIndices(nodes, pod, tr, nil)
		ranked := RankIndexed(nodes, idx, w, "balanced", rc)
		if len(ranked) > 0 && len(CheckViolations(pod, ranked[0].Node)) == 0 {
			for _, r := range ranked {
				if len(CheckViolations(pod, r.Node)) > 0 {
					t.Fatalf("pod %d: gates off, a breaking node was available but not ranked first", p)
				}
			}
		}
	}
}

func TestEqualAndDefaultScorerWiring(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	nodes := randomInventory(rng, 400)
	w := AHPWeights{CPU: 0.35, Memory: 0.25, Storage: 0.15, IO: 0.15, Cache: 0.10}
	pod := randomPod(rng)
	tr := NewNodeResourceTracker()
	for k := 0; k < 300; k++ {
		n := nodes[rng.Intn(len(nodes))]
		tr.Reserve(n.Name, randomPod(rng), n)
	}
	for _, prio := range []string{"balanced", "privacy", "performance"} {
		sameRanking(t, "equal", RankNodesFor(nodes, EqualAHPWeights, "balanced", RankContext{}),
			RankNodesFor(nodes, w, prio, RankContext{Scorer: ScorerEqual, Pod: &pod, Usage: tr}))
		sameRanking(t, "default", RankNodesFor(nodes, w, prio, RankContext{}),
			RankNodesFor(nodes, w, prio, RankContext{Scorer: ScorerAHP, Pod: &pod, Usage: tr}))
	}
}

func TestParseScorer(t *testing.T) {
	for in, want := range map[string]Scorer{"": ScorerAHP, "ahp": ScorerAHP, "equal": ScorerEqual, "k8s": ScorerK8s, "adversarial": ScorerAdversarial} {
		if got, err := ParseScorer(in); err != nil || got != want {
			t.Errorf("ParseScorer(%q) = %q, %v", in, got, err)
		}
	}
	if _, err := ParseScorer("topsis"); err == nil {
		t.Error("an unknown scorer must be refused")
	}
}
