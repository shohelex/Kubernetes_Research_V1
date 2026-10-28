package benchmark

import (
	"fmt"
	"sort"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/auditlog"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

type decisionRecord struct {
	path      string
	rankingID uint64
	rankedFor string
	excluded  scheduler.ExclusionCounts
	drops     int
}

func permittedRegions() []string {
	var r []string
	for region, ok := range scheduler.GDPRCompliantRegions {
		if ok {
			r = append(r, string(region))
		}
	}
	sort.Strings(r)
	return r
}

func (d decisionRecord) entry(instance int, pi generators.PodInfo, pod types.Pod, node types.Node) auditlog.Entry {
	e := auditlog.Entry{
		Kind: "decision", Instance: instance, Namespace: pi.Namespace, Pod: pi.Name,
		Residency: pod.GDPRRequired, Isolation: pod.IsolationRequired, Encryption: pod.EncryptionRequired,
		Path: d.path, RankingID: d.rankingID, RankedFor: d.rankedFor, Excluded: d.excluded.Map(),
		Node: node.Name, NodeLabels: labelsOf(node), RecheckDrops: d.drops,
	}
	if pod.GDPRRequired {
		e.Permitted = permittedRegions()
	}
	return e
}

func (d decisionRecord) unschedulable(instance int, pi generators.PodInfo, pod types.Pod) auditlog.Entry {
	e := auditlog.Entry{
		Kind: "decision", Instance: instance, Namespace: pi.Namespace, Pod: pi.Name,
		Residency: pod.GDPRRequired, Isolation: pod.IsolationRequired, Encryption: pod.EncryptionRequired,
		Path: d.path, Excluded: d.excluded.Map(), Outcome: "unschedulable",
	}
	if pod.GDPRRequired {
		e.Permitted = permittedRegions()
	}
	return e
}

func labelsOf(n types.Node) *auditlog.Labels {
	return &auditlog.Labels{Region: string(n.Region), Isolation: string(n.Isolation),
		EncryptedStorage: n.EncryptedStorage != nil, EncryptedTraffic: n.EncryptedTraffic != nil}
}

func outcomeEntry(instance int, pi generators.PodInfo, pod types.Pod, node, outcome string) auditlog.Entry {
	return auditlog.Entry{Kind: "outcome", Instance: instance, Namespace: pi.Namespace, Pod: pi.Name,
		Residency: pod.GDPRRequired, Isolation: pod.IsolationRequired, Encryption: pod.EncryptionRequired,
		Node: node, Outcome: outcome}
}

func DecisionCheck(e auditlog.Entry) []string {
	if e.Outcome == "unschedulable" && e.Node == "" {
		return nil
	}
	if e.NodeLabels == nil {
		return []string{"no labels recorded"}
	}
	pod := types.Pod{GDPRRequired: e.Residency, IsolationRequired: e.Isolation, EncryptionRequired: e.Encryption}
	node := types.Node{Name: e.Node, Region: types.Region(e.NodeLabels.Region), Isolation: types.Isolation(e.NodeLabels.Isolation)}
	if e.NodeLabels.EncryptedStorage {
		v := 1.0
		node.EncryptedStorage = &v
	}
	if e.NodeLabels.EncryptedTraffic {
		node.EncryptedTraffic = &types.TrafficEncryption{}
	}
	var fails []string
	for _, v := range scheduler.CheckViolations(pod, node) {
		fails = append(fails, string(v))
	}
	return fails
}

func auditPath(base string, run int) string {
	return fmt.Sprintf("%s.run%d.jsonl", base, run+1)
}
