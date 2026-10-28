package scheduler

import (
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func referenceFilter(nodes []types.Node, pod types.Pod, tracker *NodeResourceTracker, local *ExclusionCounts, st *FilterStats) []types.Node {
	filtered := make([]types.Node, 0, len(nodes)/2)
	for i := range nodes {
		node := &nodes[i]
		st.Total++
		if pod.GDPRRequired && PrivacyFiltersEnabled {
			if !GDPRCompliantRegions[node.Region] {
				st.RejGDPR++
				local.Residency++
				continue
			}
		}
		if pod.IsolationRequired && PrivacyFiltersEnabled {
			switch node.Isolation {
			case types.IsolationDedicated, types.IsolationVmIsolated, types.IsolationNetworkPolicy:
			default:
				st.RejIsolation++
				local.Isolation++
				continue
			}
		}
		if pod.EncryptionRequired && PrivacyFiltersEnabled {
			if node.EncryptedStorage == nil && node.EncryptedTraffic == nil {
				st.RejEncryption++
				local.Encryption++
				continue
			}
		}
		if pod.CPU > 0 && node.CPUCores*1000 < pod.CPU {
			st.RejCPU++
			local.Capacity++
			continue
		}
		if pod.Memory > 0 && node.Memory*1024 < pod.Memory {
			st.RejMemory++
			local.Capacity++
			continue
		}
		if pod.Storage > 0 && node.Storage < pod.Storage {
			st.RejStorage++
			local.Capacity++
			continue
		}
		if node.Bandwidth < pod.Bandwidth {
			st.RejBandwidth++
			local.Network++
			continue
		}
		if node.IOSpeed < pod.IOSpeed {
			st.RejIOSpeed++
			local.Network++
			continue
		}
		if node.Ping > pod.Latency {
			st.RejLatency++
			local.Network++
			continue
		}
		cpuUsed, memUsed, storUsed := tracker.Get(node.Name)
		if !ResourceAdmits(cpuUsed, int64(node.CPUCores)*1000, int64(pod.CPU)) {
			st.RejCPUDepleted++
			local.Depleted++
			continue
		}
		if !ResourceAdmits(memUsed, int64(node.Memory)*1024, int64(pod.Memory)) {
			st.RejMemDepleted++
			local.Depleted++
			continue
		}
		if !ResourceAdmits(storUsed, int64(node.Storage), int64(pod.Storage)) {
			st.RejStorDepleted++
			local.Depleted++
			continue
		}
		st.Passed++
		filtered = append(filtered, *node)
	}
	return filtered
}

func referenceRank(nodes []types.Node, w AHPWeights, priority string, rc RankContext) []RankedNode {
	if len(nodes) == 0 {
		return nil
	}
	pw, ok := SchedulingPriorityWeights[priority]
	if !ok {
		pw = SchedulingPriorityWeights["balanced"]
	}
	penalty := 0.0
	if rc.HomeCluster != "" && rc.MovePenalty > 0 {
		penalty = rc.MovePenalty * (pw.Performance + pw.Exposure + pw.Isolation + pw.Encryption + pw.DPA)
	}
	var cMax, mMax, sMax, ioMax, lMax uint32
	for i := range nodes {
		if nodes[i].CPUCores > cMax {
			cMax = nodes[i].CPUCores
		}
		if nodes[i].Memory > mMax {
			mMax = nodes[i].Memory
		}
		if nodes[i].Storage > sMax {
			sMax = nodes[i].Storage
		}
		if nodes[i].IOSpeed > ioMax {
			ioMax = nodes[i].IOSpeed
		}
		if nodes[i].LCache > lMax {
			lMax = nodes[i].LCache
		}
	}
	for _, p := range []*uint32{&cMax, &mMax, &sMax, &ioMax, &lMax} {
		if *p == 0 {
			*p = 1
		}
	}
	ranked := make([]RankedNode, len(nodes))
	for i := range nodes {
		score := totalScoreFor(&nodes[i], w, pw, cMax, mMax, sMax, ioMax, lMax, rc.DataRegion)
		if penalty > 0 && nodes[i].Cluster != rc.HomeCluster {
			score -= penalty
		}
		ranked[i] = RankedNode{Node: nodes[i], Score: score}
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].Score > ranked[j].Score })
	return ranked
}

func randomInventory(rng *rand.Rand, n int) []types.Node {
	pick := func(vs ...uint32) uint32 { return vs[rng.Intn(len(vs))] }
	isos := []types.Isolation{types.IsolationNone, types.IsolationNamespace, types.IsolationNetworkPolicy, types.IsolationDedicated, types.IsolationVmIsolated}
	regions := []types.Region{types.RegionEurope, types.RegionUS, types.RegionAsia, types.RegionOther}
	nodes := make([]types.Node, n)
	for i := range nodes {
		nd := types.Node{
			Name:     "node-" + string(rune('a'+i%26)) + "-" + strconv.Itoa(i),
			CPUCores: pick(4, 8, 16, 32), Memory: pick(8, 16, 32, 64), Storage: pick(100, 200, 500),
			Bandwidth: pick(100, 1000), IOSpeed: pick(100, 500), Ping: pick(5, 20, 50), LCache: pick(8, 16, 32),
			Exposure:  []types.Exposure{types.ExposurePrivate, types.ExposurePublic}[rng.Intn(2)],
			Isolation: isos[rng.Intn(len(isos))], Region: regions[rng.Intn(len(regions))],
			Cluster: []string{"c0", "c1"}[rng.Intn(2)],
		}
		if rng.Intn(2) == 0 {
			d := []float64{0.5, 1.0}[rng.Intn(2)]
			nd.EncryptedStorage = &d
		}
		if rng.Intn(3) == 0 {
			nd.EncryptedTraffic = &types.TrafficEncryption{Dt: 1, Ispeed: 100, Pl: 0.01, Lat: 20, Jit: 5}
		}
		nodes[i] = nd
	}

	for i := range nodes {
		if i >= 12 && rng.Intn(4) != 0 {
			name := nodes[i].Name
			nodes[i] = nodes[rng.Intn(12)]
			nodes[i].Name = name
		}
	}
	return nodes
}

func randomPod(rng *rand.Rand) types.Pod {
	return types.Pod{
		CPU: uint32(500 * (1 + rng.Intn(24))), Memory: uint32(512 * (1 + rng.Intn(64))), Storage: uint32(rng.Intn(300)),
		Bandwidth: uint32(rng.Intn(1200)), IOSpeed: uint32(rng.Intn(600)), Latency: uint32(10 + rng.Intn(60)),
		GDPRRequired: rng.Intn(2) == 0, IsolationRequired: rng.Intn(3) == 0, EncryptionRequired: rng.Intn(3) == 0,
	}
}

func sameRanking(t *testing.T, what string, want, got []RankedNode) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: %d nodes, want %d", what, len(got), len(want))
	}
	for i := range want {
		if want[i].Node.Name != got[i].Node.Name || want[i].Score != got[i].Score {
			t.Fatalf("%s: position %d is %s (%.17g), want %s (%.17g)", what, i,
				got[i].Node.Name, got[i].Score, want[i].Node.Name, want[i].Score)
		}
		if want[i].Node != got[i].Node {
			t.Fatalf("%s: position %d: node fields differ", what, i)
		}
	}
}

func TestFilterRankEquivalent(t *testing.T) {
	w := AHPWeights{CPU: 0.35, Memory: 0.25, Storage: 0.15, IO: 0.15, Cache: 0.10}
	contexts := []RankContext{{}, {HomeCluster: "c0", DataRegion: types.RegionEurope, MovePenalty: 0.1}}
	ties, compared := 0, 0
	for _, privacy := range []bool{true, false} {
		saved := PrivacyFiltersEnabled
		PrivacyFiltersEnabled = privacy
		for seed := int64(1); seed <= 4; seed++ {
			rng := rand.New(rand.NewSource(seed))
			nodes := randomInventory(rng, 1500)
			tracker := NewNodeResourceTracker()
			if seed%2 == 0 {
				names := make([]string, len(nodes))
				for i := range nodes {
					names[i] = nodes[i].Name
				}
				tracker.Reset(names...)
			}
			for k := 0; k < 3000; k++ {
				n := nodes[rng.Intn(len(nodes))]
				tracker.Reserve(n.Name, randomPod(rng), n)
			}
			for p := 0; p < 250; p++ {
				pod := randomPod(rng)
				var refEx, cntEx, idxEx ExclusionCounts
				var refSt FilterStats
				ref := referenceFilter(nodes, pod, tracker, &refEx, &refSt)

				ResetFilterStats()
				cnt := FilterNodesCounted(nodes, pod, tracker, &cntEx)
				cntSt := GlobalFilterStats
				ResetFilterStats()
				idx := FilterIndices(nodes, pod, tracker, &idxEx)
				idxSt := GlobalFilterStats

				if refEx != cntEx || refEx != idxEx {
					t.Fatalf("seed %d pod %d: exclusions %+v / %+v, want %+v", seed, p, cntEx, idxEx, refEx)
				}
				if refSt != cntSt || refSt != idxSt {
					t.Fatalf("seed %d pod %d: filter statistics differ:\n got %+v\n got %+v\nwant %+v", seed, p, cntSt, idxSt, refSt)
				}
				if len(ref) != len(cnt) || len(ref) != len(idx) {
					t.Fatalf("seed %d pod %d: %d / %d nodes pass, want %d", seed, p, len(cnt), len(idx), len(ref))
				}
				for i := range ref {
					if ref[i] != cnt[i] || ref[i] != nodes[idx[i]] {
						t.Fatalf("seed %d pod %d: filtered node %d differs", seed, p, i)
					}
				}
				for _, rc := range contexts {
					for _, prio := range []string{"balanced", "privacy", "performance"} {
						want := referenceRank(ref, w, prio, rc)
						sameRanking(t, "RankNodesFor", want, RankNodesFor(cnt, w, prio, rc))
						sameRanking(t, "RankIndexed", want, RankIndexed(nodes, idx, w, prio, rc))
						for i := 1; i < len(want); i++ {
							if want[i].Score == want[i-1].Score {
								ties++
							}
						}
						compared += len(want)
					}

					full := RankContext{HomeCluster: rc.HomeCluster, DataRegion: rc.DataRegion, MovePenalty: rc.MovePenalty,
						Scorer: ScorerAHP, Pod: &pod, Usage: tracker}
					sameRanking(t, "RankIndexed, ahp with pod and usage", referenceRank(ref, w, "balanced", rc),
						RankIndexed(nodes, idx, w, "balanced", full))
				}
			}
		}
		PrivacyFiltersEnabled = saved
	}

	if ties < compared/10 {
		t.Fatalf("only %d tied neighbours among %d ranked nodes: the inventory does not exercise ties", ties, compared)
	}
	t.Logf("%d ranked nodes compared, %d of them tied with their predecessor", compared, ties)
}

func TestRankIndexedEmpty(t *testing.T) {
	nodes := randomInventory(rand.New(rand.NewSource(9)), 10)
	if r := RankIndexed(nodes, nil, AHPWeights{}, "balanced", RankContext{}); r != nil {
		t.Fatalf("no index ranks no node, got %d", len(r))
	}
	if r := RankIndexed(nodes, []int32{}, AHPWeights{}, "balanced", RankContext{}); r != nil {
		t.Fatalf("an empty index ranks no node, got %d", len(r))
	}
}

func TestTrackerConcurrentBookings(t *testing.T) {
	node := types.Node{Name: "n0", CPUCores: 100, Memory: 1000, Storage: 1000}
	other := types.Node{Name: "n1", CPUCores: 100, Memory: 1000, Storage: 1000}
	pod := types.Pod{CPU: 1000, Memory: 1024, Storage: 1}
	tr := NewNodeResourceTracker()
	tr.Reset("n0", "n1", "never-touched")

	var wg sync.WaitGroup
	var mu sync.Mutex
	booked := 0
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if tr.Reserve("n0", pod, node) {
					mu.Lock()
					booked++
					mu.Unlock()
				}
				tr.Get("n0")
				tr.Admits("n1", pod, other)
			}
		}()
	}
	wg.Wait()

	if booked != 91 {
		t.Fatalf("%d bookings of a 100-core node with 1-core pods, want 91", booked)
	}
	cpu, mem, stor := tr.Get("n0")
	if cpu != 91000 || mem != 91*1024 || stor != 91 {
		t.Fatalf("usage %d/%d/%d after 91 bookings", cpu, mem, stor)
	}
	snap := tr.Snapshot()
	if len(snap) != 1 || snap["n0"].PodCount != 91 {
		t.Fatalf("snapshot %+v, want only n0 with 91 pods", snap)
	}
	if pc := tr.PodCounts(); len(pc) != 1 || pc["n0"] != 91 {
		t.Fatalf("pod counts %+v", pc)
	}

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				tr.Release("n0", pod)
			}
		}()
	}
	wg.Wait()
	if cpu, mem, stor := tr.Get("n0"); cpu != 0 || mem != 0 || stor != 0 {
		t.Fatalf("usage %d/%d/%d after releasing more than was booked", cpu, mem, stor)
	}

	if !tr.Reserve("late", pod, node) {
		t.Fatal("a new node must take a pod")
	}
	if _, ok := tr.Snapshot()["late"]; !ok {
		t.Fatal("a node booked after Reset must appear in the snapshot")
	}

	tr.Release("ghost", pod)
	if _, ok := tr.Snapshot()["ghost"]; ok {
		t.Fatal("Release must not add a node")
	}
	tr.Update("upd", pod)
	if tr.PodCounts()["upd"] != 1 {
		t.Fatal("Update must add the node and count the pod")
	}
}
