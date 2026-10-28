package scheduler

import (
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func TestLiveLabelsCurrent(t *testing.T) {
	enc := 1.0
	n := types.Node{Name: "a", CPUCores: 8, Region: types.RegionEurope, Isolation: types.IsolationDedicated, EncryptedStorage: &enc}
	l := NewLiveLabels([]types.Node{n})
	l.Set("a", PrivacyLabels{Region: types.RegionUS, Isolation: types.IsolationNone})

	got := l.Current(n)
	if got.Region != types.RegionUS || got.Isolation != types.IsolationNone || got.EncryptedStorage != nil {
		t.Fatalf("privacy labels not replaced: %+v", got)
	}
	if got.CPUCores != 8 || got.Name != "a" {
		t.Fatalf("other fields changed: %+v", got)
	}
	if other := l.Current(types.Node{Name: "b", Region: types.RegionAsia}); other.Region != types.RegionAsia {
		t.Fatalf("an unknown node must keep its labels: %+v", other)
	}
}

func TestPassesGatesAgreesWithFilter(t *testing.T) {
	enc := 1.0
	var nodes []types.Node
	for _, r := range []types.Region{types.RegionEurope, types.RegionUS} {
		for _, iso := range []types.Isolation{types.IsolationNone, types.IsolationNetworkPolicy} {
			for _, e := range []*float64{nil, &enc} {
				nodes = append(nodes, types.Node{Name: "n", CPUCores: 8, Memory: 64, Storage: 100,
					Bandwidth: 1000, IOSpeed: 1000, Region: r, Isolation: iso, EncryptedStorage: e})
			}
		}
	}
	for _, g := range []bool{false, true} {
		for _, iso := range []bool{false, true} {
			for _, e := range []bool{false, true} {
				pod := types.Pod{CPU: 1000, Latency: 100, GDPRRequired: g, IsolationRequired: iso, EncryptionRequired: e}
				for _, n := range nodes {
					want := len(FilterNodes([]types.Node{n}, pod)) == 1
					if got := PassesGates(pod, n); got != want {
						t.Fatalf("pod %+v node %+v: PassesGates=%v, filter=%v", pod, n, got, want)
					}
				}
			}
		}
	}
}

func TestFilterUsesLiveLabels(t *testing.T) {
	n := types.Node{Name: "a", CPUCores: 8, Memory: 64, Storage: 100, Bandwidth: 1000, IOSpeed: 1000, Region: types.RegionEurope}
	pod := types.Pod{CPU: 1000, Latency: 100, GDPRRequired: true}
	defer func() { Live = nil }()
	Live = NewLiveLabels([]types.Node{n})
	if len(FilterNodes([]types.Node{n}, pod)) != 1 {
		t.Fatal("a European node must pass a residency-bound pod")
	}
	Live.Set("a", PrivacyLabels{Region: types.RegionUS})
	if len(FilterNodes([]types.Node{n}, pod)) != 0 || len(FilterNodesWithResources([]types.Node{n}, pod, NewNodeResourceTracker())) != 0 {
		t.Fatal("after the node left Europe both filters must reject it")
	}
}
