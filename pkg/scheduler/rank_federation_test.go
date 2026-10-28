package scheduler

import (
	"math"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestTransferMatch(t *testing.T) {
	cases := []struct {
		r, d types.Region
		want float64
	}{
		{types.RegionEurope, "", 1.0},
		{types.RegionAsia, "", 1.0},
		{types.RegionEurope, types.RegionEurope, 1.0},
		{types.RegionUS, types.RegionEurope, 0.8},
		{types.RegionAsia, types.RegionEurope, 0.5},
		{types.RegionOther, types.RegionEurope, 0.2},
		{types.RegionUS, types.RegionUS, 1.0},
		{types.RegionEurope, types.RegionUS, 0.2},
		{types.RegionEurope, types.RegionAsia, 0.2},
		{types.RegionUS, types.RegionAsia, 0.2},
	}
	for _, c := range cases {
		if got := TransferMatch(c.r, c.d); !near(got, c.want) {
			t.Errorf("TransferMatch(%s, %s) = %v, want %v", c.r, c.d, got, c.want)
		}
	}
}

func TestDPAScoreFor(t *testing.T) {
	n := func(r types.Region) *types.Node { return &types.Node{Region: r} }
	cases := []struct {
		r, d types.Region
		want float64
	}{
		{types.RegionEurope, "", 1.0},
		{types.RegionUS, "", 0.75},
		{types.RegionAsia, "", 0.5},
		{types.RegionOther, "", 0.0},
		{types.RegionUS, types.RegionEurope, (0.4*0.8 - 0.1) / 0.4},
		{types.RegionAsia, types.RegionEurope, (0.3*0.5 - 0.1) / 0.4},
		{types.RegionEurope, types.RegionUS, 0.0},
		{types.RegionUS, types.RegionAsia, 0.0},
		{types.RegionAsia, types.RegionAsia, 0.5},
	}
	for _, c := range cases {
		if got := DPAScoreFor(n(c.r), c.d); !near(got, c.want) {
			t.Errorf("DPAScoreFor(%s, origin %s) = %v, want %v", c.r, c.d, got, c.want)
		}
	}

	for _, r := range []types.Region{types.RegionEurope, types.RegionUS, types.RegionAsia, types.RegionOther} {
		if DPAScore(n(r)) != DPAScoreFor(n(r), "") {
			t.Errorf("DPAScore(%s) differs from DPAScoreFor with no origin", r)
		}
	}
}

func twin(name, cluster string, region types.Region, cpu uint32) types.Node {
	return types.Node{Name: name, Cluster: cluster, Region: region, CPUCores: cpu, Memory: 64, Storage: 500, IOSpeed: 1000, LCache: 32,
		Exposure: types.ExposurePrivate, Isolation: types.IsolationDedicated}
}

func TestRankNodesForHomeAndPenalty(t *testing.T) {
	w := AHPWeights{CPU: 0.2, Memory: 0.2, Storage: 0.2, IO: 0.2, Cache: 0.2}
	pw := SchedulingPriorityWeights["balanced"]
	sum := pw.Performance + pw.Exposure + pw.Isolation + pw.Encryption + pw.DPA
	nodes := []types.Node{
		twin("eu-0", "eu", types.RegionEurope, 16),
		twin("us-0", "us", types.RegionUS, 16),
	}
	rc := RankContext{DataRegion: types.RegionUS, HomeCluster: "us", MovePenalty: 0.1}
	ranked := RankNodesFor(nodes, w, "balanced", rc)
	if ranked[0].Node.Name != "us-0" {
		t.Fatalf("home twin should rank first, got %s", ranked[0].Node.Name)
	}

	wantGap := pw.DPA*0.75 + 0.1*sum
	if gap := ranked[0].Score - ranked[1].Score; !near(gap, wantGap) {
		t.Errorf("score gap = %v, want %v", gap, wantGap)
	}

	if r := RankNodesFor(nodes, w, "balanced", RankContext{}); r[0].Node.Name != "eu-0" {
		t.Errorf("single-cluster ranking should prefer the European node, got %s", r[0].Node.Name)
	}

	nodes = []types.Node{
		twin("us-small", "us", types.RegionUS, 1),
		{Name: "eu-big", Cluster: "eu", Region: types.RegionEurope, CPUCores: 64, Memory: 512, Storage: 4000, IOSpeed: 8000, LCache: 64,
			Exposure: types.ExposurePrivate, Isolation: types.IsolationVmIsolated, EncryptedStorage: new(float64),
			EncryptedTraffic: &types.TrafficEncryption{Dt: 1, Ispeed: 8000, Pl: 0, Lat: 1, Jit: 0}},
	}
	nodes[0].Memory, nodes[0].Storage, nodes[0].IOSpeed, nodes[0].LCache = 1, 10, 50, 8
	nodes[0].Exposure, nodes[0].Isolation = types.ExposurePublic, types.IsolationNone
	if r := RankNodesFor(nodes, w, "balanced", rc); r[0].Node.Name != "eu-big" {
		t.Errorf("a much better node in another cluster should win, got %s", r[0].Node.Name)
	}
}

func TestCanReuseIgnoresHome(t *testing.T) {
	p := types.Pod{CPU: 4000, Memory: 8000, HomeCluster: "us", DataRegion: types.RegionUS}
	q := p
	if !CanReuseQueue(p, q) {
		t.Fatal("identical pods must reuse")
	}
	q.HomeCluster, q.DataRegion = "asia", types.RegionAsia
	if !CanReuseQueue(p, q) {
		t.Error("the reuse rule compares requirements only, not the home cluster")
	}
	q.GDPRRequired = true
	if CanReuseQueue(p, q) {
		t.Error("a stricter privacy requirement must not reuse the ranking")
	}
}
