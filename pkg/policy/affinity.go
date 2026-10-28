package policy

import (
	"sort"
	"strconv"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	corev1 "k8s.io/api/core/v1"
)

const (
	AnnotationResidency  = "placement-gates/gdpr-required"
	AnnotationIsolation  = "placement-gates/isolation-required"
	AnnotationEncryption = "placement-gates/encryption-required"
)

func Annotations(req types.Pod) map[string]string {
	if !req.GDPRRequired && !req.IsolationRequired && !req.EncryptionRequired {
		return nil
	}
	return map[string]string{
		AnnotationResidency:  strconv.FormatBool(req.GDPRRequired),
		AnnotationIsolation:  strconv.FormatBool(req.IsolationRequired),
		AnnotationEncryption: strconv.FormatBool(req.EncryptionRequired),
	}
}

func FromAnnotations(a map[string]string) types.Pod {
	return types.Pod{GDPRRequired: a[AnnotationResidency] == "true",
		IsolationRequired: a[AnnotationIsolation] == "true", EncryptionRequired: a[AnnotationEncryption] == "true"}
}

func PrivacyAffinity(req types.Pod, preferred bool) *corev1.Affinity {
	var base []corev1.NodeSelectorRequirement
	if req.GDPRRequired {
		var regions []string
		for r := range scheduler.GDPRCompliantRegions {
			regions = append(regions, generators.RegionLabelValue(r))
		}
		sort.Strings(regions)
		base = append(base, corev1.NodeSelectorRequirement{Key: "region", Operator: corev1.NodeSelectorOpIn, Values: regions})
	}
	if req.IsolationRequired {
		base = append(base, corev1.NodeSelectorRequirement{Key: "isolation", Operator: corev1.NodeSelectorOpIn,
			Values: []string{string(types.IsolationNetworkPolicy), string(types.IsolationDedicated), string(types.IsolationVmIsolated)}})
	}
	var terms []corev1.NodeSelectorTerm
	if req.EncryptionRequired {
		for _, key := range []string{"encrypted_storage", "encrypted_traffic"} {
			reqs := append(append([]corev1.NodeSelectorRequirement{}, base...),
				corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpExists})
			terms = append(terms, corev1.NodeSelectorTerm{MatchExpressions: reqs})
		}
	} else if len(base) > 0 {
		terms = []corev1.NodeSelectorTerm{{MatchExpressions: base}}
	}
	if len(terms) == 0 {
		return nil
	}
	if preferred {
		prefs := make([]corev1.PreferredSchedulingTerm, len(terms))
		for i, t := range terms {
			prefs[i] = corev1.PreferredSchedulingTerm{Weight: 100, Preference: t}
		}
		return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: prefs}}
	}
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms},
	}}
}
