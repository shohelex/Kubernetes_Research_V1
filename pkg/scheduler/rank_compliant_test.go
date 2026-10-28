package scheduler

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func TestCompliantLast(t *testing.T) {
	w := AHPWeights{CPU: 0.3, Memory: 0.2, Storage: 0.2, IO: 0.2, Cache: 0.1}
	rng := rand.New(rand.NewSource(11))
	clusters := []string{"eu", "us", "asia"}
	regions := []types.Region{types.RegionEurope, types.RegionUS, types.RegionAsia, types.RegionOther}
	var nodes []types.Node
	for i := 0; i < 200; i++ {
		n := types.Node{Name: fmt.Sprintf("n-%d", i), Cluster: clusters[i%3], Region: regions[rng.Intn(4)],
			CPUCores: uint32(1 + rng.Intn(64)), Memory: uint32(1 + rng.Intn(512)), Storage: uint32(10 + rng.Intn(4000)),
			IOSpeed: uint32(50 + rng.Intn(8000)), LCache: uint32(8 + rng.Intn(57)), Isolation: types.IsolationDedicated,
			Exposure: types.ExposurePrivate}
		nodes = append(nodes, n)
	}
	pod := types.Pod{Name: "p", CPU: 1000, Memory: 256}
	tracker := NewNodeResourceTracker()
	compliant := func(n types.Node) bool { return GDPRCompliantRegions[n.Region] }
	scores := func(rs []RankedNode, keep func(types.Node) bool) []string {
		var out []string
		for _, r := range rs {
			if keep(r.Node) {
				out = append(out, fmt.Sprintf("%.9f", r.Score))
			}
		}
		return out
	}
	same := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	for _, scorer := range []Scorer{ScorerAHP, ScorerEqual, ScorerK8s, ScorerAdversarial} {
		for _, home := range []string{"", "eu", "us"} {
			rc := RankContext{Scorer: scorer, Pod: &pod, Usage: tracker, DataRegion: types.RegionEurope, HomeCluster: home}
			if home != "" {
				rc.MovePenalty = DefaultMovePenalty
			}
			plain := RankNodesFor(nodes, w, "balanced", rc)
			rc.CompliantLast = true
			last := RankNodesFor(nodes, w, "balanced", rc)
			if len(last) != len(plain) {
				t.Fatalf("%s, home %q: %d nodes ranked with the option, %d without", scorer, home, len(last), len(plain))
			}
			seen := false
			for _, r := range last {
				if compliant(r.Node) {
					seen = true
				} else if seen {
					t.Fatalf("%s, home %q: node %s outside the compliant regions ranks after a compliant node", scorer, home, r.Node.Name)
				}
			}
			for _, part := range []func(types.Node) bool{compliant, func(n types.Node) bool { return !compliant(n) }} {
				if !same(scores(last, part), scores(plain, part)) {
					t.Fatalf("%s, home %q: the option changed the order within a part", scorer, home)
				}
			}
		}
	}
}
