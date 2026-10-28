package scheduler

import (
	"sort"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

var SchedulingPriorityWeights = map[string]PriorityWeights{
	"privacy": {
		Performance: 0.5,
		Exposure:    2.0,
		Isolation:   2.0,
		Encryption:  2.0,
		DPA:         2.0,
	},
	"performance": {
		Performance: 3.0,
		Exposure:    0.3,
		Isolation:   0.3,
		Encryption:  0.3,
		DPA:         0.3,
	},
	"balanced": {
		Performance: 1.0,
		Exposure:    1.0,
		Isolation:   1.0,
		Encryption:  1.0,
		DPA:         1.0,
	},
}

type PriorityWeights struct {
	Performance float64
	Exposure    float64
	Isolation   float64
	Encryption  float64
	DPA         float64
}

var IsolationScores = map[types.Isolation]float64{
	types.IsolationNone:          0.0,
	types.IsolationNamespace:     0.3,
	types.IsolationNetworkPolicy: 0.5,
	types.IsolationDedicated:     0.7,
	types.IsolationVmIsolated:    1.0,
}

var RegionScores = map[types.Region]float64{
	types.RegionEurope: 0.5,
	types.RegionUS:     0.4,
	types.RegionAsia:   0.3,
	types.RegionOther:  0.1,
}

type RankedNode struct {
	Node  types.Node
	Score float64
}

func PerformanceScore(node *types.Node, w AHPWeights, cMax, mMax, sMax, ioMax, lMax uint32) float64 {
	c := safeDiv(float64(node.CPUCores), float64(cMax))
	m := safeDiv(float64(node.Memory), float64(mMax))
	s := safeDiv(float64(node.Storage), float64(sMax))
	io := safeDiv(float64(node.IOSpeed), float64(ioMax))
	l := safeDiv(float64(node.LCache), float64(lMax))

	return w.CPU*c + w.Memory*m + w.Storage*s + w.IO*io + w.Cache*l
}

func ExposureScore(node *types.Node) float64 {
	if node.Exposure == types.ExposurePrivate {
		return 1.0
	}
	return 0.0
}

func IsolationScore(node *types.Node) float64 {
	score, ok := IsolationScores[node.Isolation]
	if !ok {
		return 0.0
	}
	return score
}

func StorageEncryptionScore(node *types.Node) float64 {
	if node.EncryptedStorage == nil {
		return 0.0
	}
	const ks = 1.0
	dStorage := *node.EncryptedStorage
	rrw := float64(node.IOSpeed)
	if rrw == 0 {
		return 0.0
	}
	return ks / (1.0 + dStorage/rrw)
}

func TrafficEncryptionScore(node *types.Node) float64 {
	if node.EncryptedTraffic == nil {
		return 0.0
	}
	te := node.EncryptedTraffic

	const (
		alphaPL = 0.5
		alphaL  = 0.4
		alphaJ  = 0.1
		plMax   = 0.05
		lMax    = 200.0
		jMax    = 50.0
	)

	reliability := alphaPL*(1.0-te.Pl/plMax) +
		alphaL*(1.0-te.Lat/lMax) +
		alphaJ*(1.0-te.Jit/jMax)

	if te.Ispeed == 0 {
		return 0.0
	}
	return reliability / (1.0 + te.Dt/te.Ispeed)
}

func TransferMatch(r, d types.Region) float64 {
	if d == "" || r == d {
		return 1.0
	}
	if GDPRCompliantRegions[d] {
		switch {
		case GDPRCompliantRegions[r]:
			return 1.0
		case r == types.RegionUS:
			return 0.8
		case r == types.RegionAsia:
			return 0.5
		}
	}
	return 0.2
}

func DPAScore(node *types.Node) float64 {
	return DPAScoreFor(node, "")
}

func DPAScoreFor(node *types.Node, dataRegion types.Region) float64 {
	s, ok := RegionScores[node.Region]
	if !ok {
		s = 0.1
	}
	w := s * TransferMatch(node.Region, dataRegion)
	v := (w - 0.1) / (0.5 - 0.1)
	if v < 0 {
		return 0
	}
	return v
}

func totalScoreFor(node *types.Node, w AHPWeights, pw PriorityWeights,
	cMax, mMax, sMax, ioMax, lMax uint32, dataRegion types.Region) float64 {
	sPerf := PerformanceScore(node, w, cMax, mMax, sMax, ioMax, lMax)
	sExposure := ExposureScore(node)
	sIsolation := IsolationScore(node)
	sEncStor := StorageEncryptionScore(node)
	sEncTraffic := TrafficEncryptionScore(node)
	sDPA := DPAScoreFor(node, dataRegion)

	return pw.Performance*sPerf +
		pw.Exposure*sExposure +
		pw.Isolation*sIsolation +
		pw.Encryption*(sEncStor+sEncTraffic) +
		pw.DPA*sDPA
}

const DefaultMovePenalty = 2.0

type RankContext struct {
	DataRegion types.Region

	HomeCluster string
	MovePenalty float64

	CompliantLast bool

	Scorer Scorer
	Pod    *types.Pod
	Usage  *NodeResourceTracker
}

func RankNodesFor(nodes []types.Node, w AHPWeights, priority string, rc RankContext) []RankedNode {
	return rankIndexed(nodes, nil, w, priority, rc)
}

func RankIndexed(nodes []types.Node, idx []int32, w AHPWeights, priority string, rc RankContext) []RankedNode {
	if idx == nil {
		idx = []int32{}
	}
	return rankIndexed(nodes, idx, w, priority, rc)
}

func rankIndexed(all []types.Node, idx []int32, w AHPWeights, priority string, rc RankContext) []RankedNode {
	n := len(all)
	if idx != nil {
		n = len(idx)
	}
	if n == 0 {
		return nil
	}
	at := func(k int) *types.Node {
		if idx == nil {
			return &all[k]
		}
		return &all[idx[k]]
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
	for k := 0; k < n; k++ {
		node := at(k)
		if node.CPUCores > cMax {
			cMax = node.CPUCores
		}
		if node.Memory > mMax {
			mMax = node.Memory
		}
		if node.Storage > sMax {
			sMax = node.Storage
		}
		if node.IOSpeed > ioMax {
			ioMax = node.IOSpeed
		}
		if node.LCache > lMax {
			lMax = node.LCache
		}
	}

	if cMax == 0 {
		cMax = 1
	}
	if mMax == 0 {
		mMax = 1
	}
	if sMax == 0 {
		sMax = 1
	}
	if ioMax == 0 {
		ioMax = 1
	}
	if lMax == 0 {
		lMax = 1
	}

	var alt func(node *types.Node) float64
	switch rc.Scorer {
	case ScorerEqual:
		epw := SchedulingPriorityWeights["balanced"]
		alt = func(node *types.Node) float64 {
			return totalScoreFor(node, EqualAHPWeights, epw, cMax, mMax, sMax, ioMax, lMax, rc.DataRegion)
		}
	case ScorerK8s:
		alt = func(node *types.Node) float64 { return k8sScore(node, rc.Pod, rc.Usage) }
	case ScorerAdversarial:
		alt = func(node *types.Node) float64 {
			return adversarialScore(node, rc.Pod, totalScoreFor(node, w, pw, cMax, mMax, sMax, ioMax, lMax, rc.DataRegion))
		}
	}

	type scored struct {
		score float64
		k     int32
		last  bool
	}
	sc := make([]scored, n)
	for k := 0; k < n; k++ {
		node := at(k)
		var score float64
		if alt == nil {
			score = totalScoreFor(node, w, pw, cMax, mMax, sMax, ioMax, lMax, rc.DataRegion)
		} else {
			score = alt(node)
		}
		if penalty > 0 && node.Cluster != rc.HomeCluster {
			score -= penalty
		}
		sc[k] = scored{score: score, k: int32(k), last: rc.CompliantLast && GDPRCompliantRegions[node.Region]}
	}

	if rc.CompliantLast {
		sort.Slice(sc, func(i, j int) bool {
			if sc[i].last != sc[j].last {
				return !sc[i].last
			}
			return sc[i].score > sc[j].score
		})
	} else {
		sort.Slice(sc, func(i, j int) bool {
			return sc[i].score > sc[j].score
		})
	}

	ranked := make([]RankedNode, n)
	for i := range sc {
		ranked[i] = RankedNode{Node: *at(int(sc[i].k)), Score: sc[i].score}
	}
	return ranked
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}
