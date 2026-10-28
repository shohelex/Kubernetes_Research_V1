package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

type NodeResources struct {
	CPUUsed  int64
	MemUsed  int64
	StorUsed int64
	PodCount int
}

type NodeResourceTracker struct {
	mu    sync.Mutex
	nodes atomic.Pointer[map[string]*nodeUsage]
}

type nodeUsage struct {
	cpu, mem, stor, pods atomic.Int64
	touched              atomic.Bool
}

func NewNodeResourceTracker() *NodeResourceTracker {
	t := &NodeResourceTracker{}
	m := map[string]*nodeUsage{}
	t.nodes.Store(&m)
	return t
}

func (t *NodeResourceTracker) entry(nodeName string) *nodeUsage {
	m := *t.nodes.Load()
	if u, ok := m[nodeName]; ok {
		return u
	}
	next := make(map[string]*nodeUsage, len(m)+1)
	for k, v := range m {
		next[k] = v
	}
	u := &nodeUsage{}
	next[nodeName] = u
	t.nodes.Store(&next)
	return u
}

func (t *NodeResourceTracker) Update(nodeName string, pod types.Pod) {
	t.mu.Lock()
	u := t.entry(nodeName)
	u.touched.Store(true)
	u.cpu.Add(int64(pod.CPU))
	u.mem.Add(int64(pod.Memory))
	u.stor.Add(int64(pod.Storage))
	u.pods.Add(1)
	t.mu.Unlock()
}

func (t *NodeResourceTracker) Get(nodeName string) (cpuUsed, memUsed, storUsed int64) {
	if u, ok := (*t.nodes.Load())[nodeName]; ok {
		return u.cpu.Load(), u.mem.Load(), u.stor.Load()
	}
	return 0, 0, 0
}

func (t *NodeResourceTracker) Reset(nodeNames ...string) {
	m := make(map[string]*nodeUsage, len(nodeNames))
	for _, n := range nodeNames {
		m[n] = &nodeUsage{}
	}
	t.mu.Lock()
	t.nodes.Store(&m)
	t.mu.Unlock()
}

var GDPRCompliantRegions = map[types.Region]bool{
	"Europe":         true,
	"EU":             true,
	"EEA":            true,
	"European Union": true,
}

func FilterNodes(nodes []types.Node, pod types.Pod) []types.Node {
	filtered := make([]types.Node, 0, len(nodes)/2)

	for i := range nodes {
		node := &nodes[i]
		if Live != nil {
			cur := Live.Current(*node)
			node = &cur
		}

		if pod.GDPRRequired && PrivacyFiltersEnabled {
			if !GDPRCompliantRegions[node.Region] {
				continue
			}
		}

		if pod.IsolationRequired && PrivacyFiltersEnabled {
			switch node.Isolation {
			case types.IsolationDedicated, types.IsolationVmIsolated, types.IsolationNetworkPolicy:

			default:
				continue
			}
		}

		if pod.EncryptionRequired && PrivacyFiltersEnabled {
			if node.EncryptedStorage == nil && node.EncryptedTraffic == nil {
				continue
			}
		}

		if pod.CPU > 0 && node.CPUCores*1000 < pod.CPU {
			continue
		}

		if pod.Memory > 0 && node.Memory*1024 < pod.Memory {
			continue
		}

		if pod.Storage > 0 && node.Storage < pod.Storage {
			continue
		}

		if node.Bandwidth < pod.Bandwidth {
			continue
		}

		if node.IOSpeed < pod.IOSpeed {
			continue
		}

		if node.Ping > pod.Latency {
			continue
		}

		filtered = append(filtered, *node)
	}

	return filtered
}

type FilterStats struct {
	Total           int64
	Passed          int64
	RejGDPR         int64
	RejIsolation    int64
	RejEncryption   int64
	RejCPU          int64
	RejMemory       int64
	RejStorage      int64
	RejBandwidth    int64
	RejIOSpeed      int64
	RejLatency      int64
	RejCPUDepleted  int64
	RejMemDepleted  int64
	RejStorDepleted int64
}

func (f *FilterStats) merge(p *FilterStats) {
	for _, c := range [...]struct{ dst, v *int64 }{
		{&f.Total, &p.Total}, {&f.Passed, &p.Passed},
		{&f.RejGDPR, &p.RejGDPR}, {&f.RejIsolation, &p.RejIsolation}, {&f.RejEncryption, &p.RejEncryption},
		{&f.RejCPU, &p.RejCPU}, {&f.RejMemory, &p.RejMemory}, {&f.RejStorage, &p.RejStorage},
		{&f.RejBandwidth, &p.RejBandwidth}, {&f.RejIOSpeed, &p.RejIOSpeed}, {&f.RejLatency, &p.RejLatency},
		{&f.RejCPUDepleted, &p.RejCPUDepleted}, {&f.RejMemDepleted, &p.RejMemDepleted}, {&f.RejStorDepleted, &p.RejStorDepleted},
	} {
		if *c.v != 0 {
			atomic.AddInt64(c.dst, *c.v)
		}
	}
}

var GlobalFilterStats FilterStats

type ExclusionCounts struct {
	Residency  int
	Isolation  int
	Encryption int
	Capacity   int
	Network    int
	Depleted   int
}

func (c ExclusionCounts) Map() map[string]int {
	m := map[string]int{}
	for k, v := range map[string]int{"residency": c.Residency, "isolation": c.Isolation, "encryption": c.Encryption,
		"capacity": c.Capacity, "network": c.Network, "depleted": c.Depleted} {
		if v > 0 {
			m[k] = v
		}
	}
	return m
}

func FilterNodesWithResources(nodes []types.Node, pod types.Pod, tracker *NodeResourceTracker) []types.Node {
	return FilterNodesCounted(nodes, pod, tracker, nil)
}

func FilterNodesCounted(nodes []types.Node, pod types.Pod, tracker *NodeResourceTracker, local *ExclusionCounts) []types.Node {
	if tracker == nil {
		return FilterNodes(nodes, pod)
	}
	var none ExclusionCounts
	if local == nil {
		local = &none
	}

	var st FilterStats
	filtered := make([]types.Node, 0, len(nodes)/2)
	for i := range nodes {
		node := &nodes[i]
		if Live != nil {
			cur := Live.Current(*node)
			node = &cur
		}
		if check(node, pod, tracker, &st, local) {
			filtered = append(filtered, *node)
		}
	}
	GlobalFilterStats.merge(&st)
	return filtered
}

func FilterIndices(nodes []types.Node, pod types.Pod, tracker *NodeResourceTracker, local *ExclusionCounts) []int32 {
	var none ExclusionCounts
	if local == nil {
		local = &none
	}
	var st FilterStats
	idx := make([]int32, 0, len(nodes)/2)
	for i := range nodes {
		if check(&nodes[i], pod, tracker, &st, local) {
			idx = append(idx, int32(i))
		}
	}
	GlobalFilterStats.merge(&st)
	return idx
}

func check(node *types.Node, pod types.Pod, tracker *NodeResourceTracker, st *FilterStats, local *ExclusionCounts) bool {
	st.Total++

	if pod.GDPRRequired && PrivacyFiltersEnabled {
		if !GDPRCompliantRegions[node.Region] {
			st.RejGDPR++
			local.Residency++
			return false
		}
	}

	if pod.IsolationRequired && PrivacyFiltersEnabled {
		switch node.Isolation {
		case types.IsolationDedicated, types.IsolationVmIsolated, types.IsolationNetworkPolicy:

		default:
			st.RejIsolation++
			local.Isolation++
			return false
		}
	}

	if pod.EncryptionRequired && PrivacyFiltersEnabled {
		if node.EncryptedStorage == nil && node.EncryptedTraffic == nil {
			st.RejEncryption++
			local.Encryption++
			return false
		}
	}

	if pod.CPU > 0 && node.CPUCores*1000 < pod.CPU {
		st.RejCPU++
		local.Capacity++
		return false
	}

	if pod.Memory > 0 && node.Memory*1024 < pod.Memory {
		st.RejMemory++
		local.Capacity++
		return false
	}

	if pod.Storage > 0 && node.Storage < pod.Storage {
		st.RejStorage++
		local.Capacity++
		return false
	}

	if node.Bandwidth < pod.Bandwidth {
		st.RejBandwidth++
		local.Network++
		return false
	}

	if node.IOSpeed < pod.IOSpeed {
		st.RejIOSpeed++
		local.Network++
		return false
	}

	if node.Ping > pod.Latency {
		st.RejLatency++
		local.Network++
		return false
	}

	cpuUsed, memUsed, storUsed := tracker.Get(node.Name)
	if !ResourceAdmits(cpuUsed, int64(node.CPUCores)*1000, int64(pod.CPU)) {
		st.RejCPUDepleted++
		local.Depleted++
		return false
	}
	if !ResourceAdmits(memUsed, int64(node.Memory)*1024, int64(pod.Memory)) {
		st.RejMemDepleted++
		local.Depleted++
		return false
	}
	if !ResourceAdmits(storUsed, int64(node.Storage), int64(pod.Storage)) {
		st.RejStorDepleted++
		local.Depleted++
		return false
	}

	st.Passed++
	return true
}

func ResetFilterStats() {
	GlobalFilterStats = FilterStats{}
}

func PrintFilterStats() {
	s := GlobalFilterStats
	fmt.Println("  Filter Rejection Breakdown:")
	fmt.Printf("    Total node checks:     %d\n", s.Total)
	fmt.Printf("    Passed:                %d (%.1f%%)\n", s.Passed, 100*float64(s.Passed)/float64(max(s.Total, 1)))
	fmt.Printf("    C1 GDPR region:        %d\n", s.RejGDPR)
	fmt.Printf("    C2 Isolation level:    %d\n", s.RejIsolation)
	fmt.Printf("    C3 Encryption:         %d\n", s.RejEncryption)
	fmt.Printf("    C4 CPU capacity:       %d\n", s.RejCPU)
	fmt.Printf("    C5 Memory capacity:    %d\n", s.RejMemory)
	fmt.Printf("    C6 Storage capacity:   %d\n", s.RejStorage)
	fmt.Printf("    C7 Bandwidth:          %d\n", s.RejBandwidth)
	fmt.Printf("    C8 IO Speed:           %d\n", s.RejIOSpeed)
	fmt.Printf("    C9 Latency:            %d\n", s.RejLatency)
	fmt.Printf("    C10 CPU depleted:      %d\n", s.RejCPUDepleted)
	fmt.Printf("    C11 Memory depleted:   %d\n", s.RejMemDepleted)
	fmt.Printf("    C12 Storage depleted:  %d\n", s.RejStorDepleted)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
