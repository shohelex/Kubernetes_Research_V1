package scheduler

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func TestHomeFirst(t *testing.T) {
	w := AHPWeights{CPU: 0.3, Memory: 0.2, Storage: 0.2, IO: 0.2, Cache: 0.1}
	noDelay := 0.0
	best := types.Node{Name: "best", Cluster: "eu", Region: types.RegionEurope, CPUCores: 64, Memory: 512, Storage: 4000,
		IOSpeed: 8000, LCache: 64, Exposure: types.ExposurePrivate, Isolation: types.IsolationVmIsolated,
		EncryptedStorage: &noDelay, EncryptedTraffic: &types.TrafficEncryption{Ispeed: 8000}}
	worst := types.Node{Name: "worst", Cluster: "us", Region: types.RegionOther, CPUCores: 1, Memory: 1, Storage: 10,
		IOSpeed: 50, LCache: 8, Exposure: types.ExposurePublic, Isolation: types.IsolationNone}
	nodes := []types.Node{best, worst}

	rng := rand.New(rand.NewSource(7))
	clusters := []string{"us", "eu", "asia"}
	regions := []types.Region{types.RegionUS, types.RegionEurope, types.RegionAsia, types.RegionOther}
	isolations := []types.Isolation{types.IsolationNone, types.IsolationNamespace, types.IsolationNetworkPolicy,
		types.IsolationDedicated, types.IsolationVmIsolated}
	for i := 0; i < 300; i++ {
		n := types.Node{Name: fmt.Sprintf("n-%d", i), Cluster: clusters[i%3], Region: regions[rng.Intn(4)],
			CPUCores: uint32(1 + rng.Intn(64)), Memory: uint32(1 + rng.Intn(512)), Storage: uint32(10 + rng.Intn(4000)),
			IOSpeed: uint32(50 + rng.Intn(8000)), LCache: uint32(8 + rng.Intn(57)), Isolation: isolations[rng.Intn(5)]}
		if rng.Intn(2) == 0 {
			n.Exposure = types.ExposurePrivate
		} else {
			n.Exposure = types.ExposurePublic
		}
		if rng.Float64() < 0.9 {
			d := float64(rng.Intn(101))
			n.EncryptedStorage = &d
		}
		if rng.Float64() < 0.9 {
			n.EncryptedTraffic = &types.TrafficEncryption{Dt: float64(1 + rng.Intn(10)), Ispeed: float64(n.IOSpeed), Pl: 0.01,
				Lat: float64(10 + rng.Intn(41)), Jit: float64(1 + rng.Intn(10))}
		}
		nodes = append(nodes, n)
	}
	for _, priority := range []string{"privacy", "performance", "balanced"} {
		for _, scorer := range []Scorer{ScorerAHP, ScorerEqual} {
			for _, origin := range []types.Region{"", types.RegionUS, types.RegionEurope} {
				rc := RankContext{DataRegion: origin, HomeCluster: "us", MovePenalty: DefaultMovePenalty, Scorer: scorer}
				ranked := RankNodesFor(nodes, w, priority, rc)
				away := false
				for _, r := range ranked {
					if r.Node.Cluster != "us" {
						away = true
					} else if away {
						t.Fatalf("%s, %s, origin %q: home node %s (%.3f) ranks below a node elsewhere", priority, scorer, origin,
							r.Node.Name, r.Score)
					}
				}
			}
		}
	}
}
