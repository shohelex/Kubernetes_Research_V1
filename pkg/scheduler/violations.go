package scheduler

import (
	"fmt"
	"sort"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

var PrivacyFiltersEnabled = true

type ViolationKind string

const (
	ViolationResidency  ViolationKind = "residency"
	ViolationIsolation  ViolationKind = "isolation"
	ViolationEncryption ViolationKind = "encryption"
	ViolationCapacity   ViolationKind = "capacity"
)

func CheckViolations(pod types.Pod, node types.Node) []ViolationKind {
	var v []ViolationKind

	if pod.GDPRRequired && !GDPRCompliantRegions[node.Region] {
		v = append(v, ViolationResidency)
	}

	if pod.IsolationRequired {
		switch node.Isolation {
		case types.IsolationDedicated, types.IsolationVmIsolated, types.IsolationNetworkPolicy:

		default:
			v = append(v, ViolationIsolation)
		}
	}

	if pod.EncryptionRequired && node.EncryptedStorage == nil && node.EncryptedTraffic == nil {
		v = append(v, ViolationEncryption)
	}

	return v
}

type ViolationReport struct {
	GDPRPods       int `json:"gdpr_pods"`
	IsolationPods  int `json:"isolation_pods"`
	EncryptionPods int `json:"encryption_pods"`

	Residency  int `json:"residency_violations"`
	Isolation  int `json:"isolation_violations"`
	Encryption int `json:"encryption_violations"`

	PodsViolating int `json:"pods_violating"`
	PodsChecked   int `json:"pods_checked"`

	UnconstrainedOnCompliant    int   `json:"unconstrained_on_compliant"`
	UnconstrainedOnCompliantCPU int64 `json:"unconstrained_on_compliant_cpu_m"`

	ResidencyViolationRate float64 `json:"residency_violation_rate"`

	OverallViolationRate float64 `json:"overall_violation_rate"`

	Examples []string `json:"examples,omitempty"`
}

type ViolationCounter struct {
	r        ViolationReport
	examples []string
}

func (c *ViolationCounter) Record(pod types.Pod, node types.Node) {
	hasReq := pod.GDPRRequired || pod.IsolationRequired || pod.EncryptionRequired
	if pod.GDPRRequired {
		c.r.GDPRPods++
	}
	if pod.IsolationRequired {
		c.r.IsolationPods++
	}
	if pod.EncryptionRequired {
		c.r.EncryptionPods++
	}
	if hasReq {
		c.r.PodsChecked++
	}
	if !pod.GDPRRequired && GDPRCompliantRegions[node.Region] {
		c.r.UnconstrainedOnCompliant++
		c.r.UnconstrainedOnCompliantCPU += int64(pod.CPU)
	}

	vs := CheckViolations(pod, node)
	if len(vs) == 0 {
		return
	}
	c.r.PodsViolating++
	for _, v := range vs {
		switch v {
		case ViolationResidency:
			c.r.Residency++
		case ViolationIsolation:
			c.r.Isolation++
		case ViolationEncryption:
			c.r.Encryption++
		}
	}
	if len(c.examples) < 5 {
		c.examples = append(c.examples, fmt.Sprintf(
			"pod %s (gdpr=%v iso=%v enc=%v) -> node %s (region=%s iso=%s encStorage=%v encTraffic=%v): %v",
			pod.Name, pod.GDPRRequired, pod.IsolationRequired, pod.EncryptionRequired,
			node.Name, node.Region, node.Isolation,
			node.EncryptedStorage != nil, node.EncryptedTraffic != nil, vs))
	}
}

func (c *ViolationCounter) Merge(o ViolationCounter) {
	c.r.GDPRPods += o.r.GDPRPods
	c.r.IsolationPods += o.r.IsolationPods
	c.r.EncryptionPods += o.r.EncryptionPods
	c.r.Residency += o.r.Residency
	c.r.Isolation += o.r.Isolation
	c.r.Encryption += o.r.Encryption
	c.r.PodsViolating += o.r.PodsViolating
	c.r.PodsChecked += o.r.PodsChecked
	c.r.UnconstrainedOnCompliant += o.r.UnconstrainedOnCompliant
	c.r.UnconstrainedOnCompliantCPU += o.r.UnconstrainedOnCompliantCPU
	c.examples = append(c.examples, o.examples...)
}

func (c *ViolationCounter) Report() ViolationReport {
	r := c.r
	if r.GDPRPods > 0 {
		r.ResidencyViolationRate = float64(r.Residency) / float64(r.GDPRPods)
	}
	if r.PodsChecked > 0 {
		r.OverallViolationRate = float64(r.PodsViolating) / float64(r.PodsChecked)
	}
	ex := append([]string(nil), c.examples...)
	sort.Strings(ex)
	if len(ex) > 5 {
		ex = ex[:5]
	}
	r.Examples = ex
	return r
}
