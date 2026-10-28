package scheduler

import (
	"math"
	"sort"
	"sync/atomic"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

type NodeUsage struct {
	Name    string  `json:"name"`
	Cluster string  `json:"cluster"`
	Region  string  `json:"region"`
	CPUUsed int64   `json:"cpu_used_milli"`
	CPUCap  int64   `json:"cpu_capacity_milli"`
	MemUsed int64   `json:"mem_used_mb"`
	MemCap  int64   `json:"mem_capacity_mb"`
	Pods    int     `json:"pods"`
	CPUUtil float64 `json:"cpu_util"`
	MemUtil float64 `json:"mem_util"`
}

type PlacementQuality struct {
	Nodes []NodeUsage `json:"nodes,omitempty"`

	CPUJain     float64 `json:"cpu_jain"`
	CPUImbal    float64 `json:"cpu_imbalance"`
	CPUCV       float64 `json:"cpu_cv"`
	MemJain     float64 `json:"mem_jain"`
	MemCV       float64 `json:"mem_cv"`
	PodJain     float64 `json:"pod_jain"`
	PodCV       float64 `json:"pod_cv"`
	MeanCPUUtil float64 `json:"mean_cpu_util"`
	MaxCPUUtil  float64 `json:"max_cpu_util"`

	NodesUsed   int `json:"nodes_used"`
	NodesEmpty  int `json:"nodes_empty"`
	NodesOver90 int `json:"nodes_over_90pct_cpu"`
	NodesTotal  int `json:"nodes_total"`

	ShadowChecked    int     `json:"shadow_checked"`
	Misplaced        int     `json:"misplaced"`
	MisplacementRate float64 `json:"misplacement_rate"`

	ScoreRegret    float64 `json:"score_regret_mean"`
	ScoreRegretMax float64 `json:"score_regret_max"`

	ScoreRegretRel float64 `json:"score_regret_rel_mean"`

	ScoreRegretRelMax float64 `json:"score_regret_rel_max"`
	ShadowNotPassing  int     `json:"shadow_not_passing"`

	ShadowRaced int `json:"shadow_raced"`
}

func (t *NodeResourceTracker) Snapshot() map[string]NodeResources {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := *t.nodes.Load()
	out := make(map[string]NodeResources, len(m))
	for k, u := range m {
		if u.touched.Load() {
			out[k] = NodeResources{CPUUsed: u.cpu.Load(), MemUsed: u.mem.Load(), StorUsed: u.stor.Load(), PodCount: int(u.pods.Load())}
		}
	}
	return out
}

var UtilizationLimit = 0.90

func ResourceAdmits(used, capacity, request int64) bool {
	if capacity > 0 && float64(used) > UtilizationLimit*float64(capacity) {
		return false
	}
	return request <= 0 || used+request <= capacity
}

func admits(cpuUsed, memUsed, storUsed int64, pod types.Pod, node types.Node) bool {
	return ResourceAdmits(cpuUsed, int64(node.CPUCores)*1000, int64(pod.CPU)) &&
		ResourceAdmits(memUsed, int64(node.Memory)*1024, int64(pod.Memory)) &&
		ResourceAdmits(storUsed, int64(node.Storage), int64(pod.Storage))
}

func (t *NodeResourceTracker) Admits(nodeName string, pod types.Pod, node types.Node) bool {
	cpu, mem, stor := t.Get(nodeName)
	return admits(cpu, mem, stor, pod, node)
}

func (t *NodeResourceTracker) Reserve(nodeName string, pod types.Pod, node types.Node) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	u := t.entry(nodeName)
	u.touched.Store(true)
	if !admits(u.cpu.Load(), u.mem.Load(), u.stor.Load(), pod, node) {
		return false
	}
	u.cpu.Add(int64(pod.CPU))
	u.mem.Add(int64(pod.Memory))
	u.stor.Add(int64(pod.Storage))
	u.pods.Add(1)
	return true
}

func (t *NodeResourceTracker) Release(nodeName string, pod types.Pod) {
	t.mu.Lock()
	defer t.mu.Unlock()
	u, ok := (*t.nodes.Load())[nodeName]
	if !ok {
		return
	}
	sub := func(v *atomic.Int64, d int64) {
		n := v.Load() - d
		if n < 0 {
			n = 0
		}
		v.Store(n)
	}
	sub(&u.cpu, int64(pod.CPU))
	sub(&u.mem, int64(pod.Memory))
	sub(&u.stor, int64(pod.Storage))
	sub(&u.pods, 1)
}

func (t *NodeResourceTracker) PodCounts() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := *t.nodes.Load()
	out := make(map[string]int, len(m))
	for k, u := range m {
		if u.touched.Load() {
			out[k] = int(u.pods.Load())
		}
	}
	return out
}

func jainAndCV(xs []float64) (jain, cv float64) {
	n := float64(len(xs))
	if n == 0 {
		return 1, 0
	}
	var sum, sumSq float64
	for _, x := range xs {
		sum += x
		sumSq += x * x
	}
	if sumSq == 0 {
		return 1, 0
	}
	jain = (sum * sum) / (n * sumSq)
	mean := sum / n
	if mean == 0 {
		return jain, 0
	}
	var varSum float64
	for _, x := range xs {
		d := x - mean
		varSum += d * d
	}
	cv = math.Sqrt(varSum/n) / mean
	return jain, cv
}

func ComputePlacementQuality(nodes []types.Node, tracker *NodeResourceTracker, includeNodes bool) PlacementQuality {
	snap := tracker.Snapshot()

	var pq PlacementQuality
	pq.NodesTotal = len(nodes)

	cpuUtil := make([]float64, 0, len(nodes))
	memUtil := make([]float64, 0, len(nodes))
	podCount := make([]float64, 0, len(nodes))

	for i := range nodes {
		n := &nodes[i]
		cpuCap := int64(n.CPUCores) * 1000
		memCap := int64(n.Memory) * 1024
		u := snap[n.Name]

		cu, mu := 0.0, 0.0
		if cpuCap > 0 {
			cu = float64(u.CPUUsed) / float64(cpuCap)
		}
		if memCap > 0 {
			mu = float64(u.MemUsed) / float64(memCap)
		}

		cpuUtil = append(cpuUtil, cu)
		memUtil = append(memUtil, mu)
		podCount = append(podCount, float64(u.PodCount))

		if u.PodCount > 0 {
			pq.NodesUsed++
		} else {
			pq.NodesEmpty++
		}
		if cu > 0.90 {
			pq.NodesOver90++
		}
		if cu > pq.MaxCPUUtil {
			pq.MaxCPUUtil = cu
		}

		if includeNodes {
			pq.Nodes = append(pq.Nodes, NodeUsage{
				Name:    n.Name,
				Cluster: n.Cluster,
				Region:  string(n.Region),
				CPUUsed: u.CPUUsed, CPUCap: cpuCap,
				MemUsed: u.MemUsed, MemCap: memCap,
				Pods:    u.PodCount,
				CPUUtil: cu, MemUtil: mu,
			})
		}
	}

	if includeNodes {
		sort.Slice(pq.Nodes, func(i, j int) bool { return pq.Nodes[i].Name < pq.Nodes[j].Name })
	}

	pq.CPUJain, pq.CPUCV = jainAndCV(cpuUtil)
	pq.CPUImbal = 1 - pq.CPUJain
	pq.MemJain, pq.MemCV = jainAndCV(memUtil)
	pq.PodJain, pq.PodCV = jainAndCV(podCount)

	if len(cpuUtil) > 0 {
		var s float64
		for _, v := range cpuUtil {
			s += v
		}
		pq.MeanCPUUtil = s / float64(len(cpuUtil))
	}

	return pq
}

type ShadowResult struct {
	Misplaced   bool
	ChosenScore float64
	BestScore   float64

	NotPassing bool

	Raced bool
}

func ShadowEvaluate(nodes []types.Node, pod types.Pod, tracker *NodeResourceTracker,
	w AHPWeights, priority string, rc RankContext, chosen string) (ShadowResult, bool) {
	candidates := FilterNodesWithResources(nodes, pod, tracker)
	if len(candidates) == 0 {
		return ShadowResult{}, false
	}
	ranked := RankNodesFor(candidates, w, priority, rc)
	var best *RankedNode
	chosenScore, chosenFound := 0.0, false
	for i := range ranked {
		n := ranked[i].Node
		if !tracker.Admits(n.Name, pod, n) {
			continue
		}
		if best == nil {
			best = &ranked[i]
		}
		if n.Name == chosen {
			chosenScore, chosenFound = ranked[i].Score, true
		}
	}
	if best == nil {
		return ShadowResult{}, false
	}
	if !chosenFound {
		for i := range nodes {
			if nodes[i].Name == chosen {
				if len(FilterNodes(nodes[i:i+1], pod)) == 1 && !tracker.Admits(chosen, pod, nodes[i]) {
					return ShadowResult{Raced: true}, true
				}
				break
			}
		}

		return ShadowResult{Misplaced: true, ChosenScore: 0, BestScore: best.Score, NotPassing: true}, true
	}
	return ShadowResult{
		Misplaced:   best.Node.Name != chosen,
		ChosenScore: chosenScore,
		BestScore:   best.Score,
	}, true
}
