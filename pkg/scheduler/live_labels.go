package scheduler

import (
	"sync"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

type PrivacyLabels struct {
	Region           types.Region
	Isolation        types.Isolation
	EncryptedStorage *float64
	EncryptedTraffic *types.TrafficEncryption
}

func PrivacyLabelsOf(n types.Node) PrivacyLabels {
	return PrivacyLabels{
		Region:           n.Region,
		Isolation:        n.Isolation,
		EncryptedStorage: n.EncryptedStorage,
		EncryptedTraffic: n.EncryptedTraffic,
	}
}

type LiveLabels struct {
	mu     sync.RWMutex
	labels map[string]PrivacyLabels
}

func NewLiveLabels(nodes []types.Node) *LiveLabels {
	l := &LiveLabels{labels: make(map[string]PrivacyLabels, len(nodes))}
	for _, n := range nodes {
		l.labels[n.Name] = PrivacyLabelsOf(n)
	}
	return l
}

func (l *LiveLabels) Set(name string, p PrivacyLabels) {
	l.mu.Lock()
	l.labels[name] = p
	l.mu.Unlock()
}

func (l *LiveLabels) Get(name string) (PrivacyLabels, bool) {
	l.mu.RLock()
	p, ok := l.labels[name]
	l.mu.RUnlock()
	return p, ok
}

func (l *LiveLabels) Current(n types.Node) types.Node {
	if p, ok := l.Get(n.Name); ok {
		n.Region, n.Isolation = p.Region, p.Isolation
		n.EncryptedStorage, n.EncryptedTraffic = p.EncryptedStorage, p.EncryptedTraffic
	}
	return n
}

var Live *LiveLabels

func CurrentNode(n types.Node) types.Node {
	if Live == nil {
		return n
	}
	return Live.Current(n)
}

func PassesGates(pod types.Pod, node types.Node) bool {
	return !PrivacyFiltersEnabled || len(CheckViolations(pod, node)) == 0
}
