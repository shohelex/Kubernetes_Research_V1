package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSameSeedSamePods(t *testing.T) {
	const n, seed, ratio = 400, 101, 0.5
	cmpClient := fake.NewSimpleClientset()
	_, reqs, _ := createBenchPods(context.Background(), cmpClient, "compare-bench", n, 4, "", "app", 0, 0, seed, ratio, true, false, false)
	cmpPods, _ := cmpClient.CoreV1().Pods("compare-bench").List(context.Background(), metav1.ListOptions{})
	cpuOf := map[string]int64{}
	for _, p := range cmpPods.Items {
		cpuOf[p.Name] = p.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
	}

	infos, _, err := generators.CreatePods(fake.NewSimpleClientset(), generators.PodGenConfig{
		Namespace: "gated-bench", Count: n, ClusterCount: 1, Profile: "synthetic", PrivacyRatio: ratio, Seed: seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	withReq := 0
	for i, info := range infos {
		name := fmt.Sprintf("compare-pod-%d", i)
		if cpuOf[name] != int64(info.CPUMilli) {
			t.Fatalf("pod %d: CPU %d here, %d in the benchmark", i, cpuOf[name], info.CPUMilli)
		}
		r := reqs[name]
		if r.GDPRRequired != info.GDPRRequired || r.IsolationRequired != info.IsolationRequired || r.EncryptionRequired != info.EncryptionRequired {
			t.Fatalf("pod %d: requirements %+v here, benchmark %+v", i, r, info)
		}
		if info.GDPRRequired {
			withReq++
		}
	}
	if withReq != len(reqs) || withReq < n/3 {
		t.Fatalf("%d pods with requirements here, %d in the benchmark", len(reqs), withReq)
	}
}

func TestAffinityMatchesGates(t *testing.T) {
	var nodes []corev1.Node
	for _, region := range []string{"Europe", "United-States"} {
		for _, iso := range []string{"none", "namespace", "network_policy", "dedicated"} {
			for _, enc := range []string{"", "storage", "traffic", "both"} {
				labels := map[string]string{"type": "kwok", "region": region, "isolation": iso}
				if enc == "storage" || enc == "both" {
					labels["encrypted_storage"] = "50"
				}
				if enc == "traffic" || enc == "both" {
					labels["encrypted_traffic"] = "1-100-0.01-10-1"
				}
				nodes = append(nodes, corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("%s-%s-%s", region, iso, enc), Labels: labels}})
			}
		}
	}
	for _, g := range []bool{false, true} {
		for _, iso := range []bool{false, true} {
			for _, enc := range []bool{false, true} {
				req := types.Pod{GDPRRequired: g, IsolationRequired: iso, EncryptionRequired: enc}
				aff := privacyAffinity(req, false)
				for i := range nodes {
					n, _ := generators.NodeFromK8s(&nodes[i])
					want := scheduler.PassesGates(req, n)
					if got := affinityAdmits(aff, nodes[i].Labels); got != want {
						t.Fatalf("pod %+v node %s: affinity %v, gates %v", req, nodes[i].Name, got, want)
					}
				}
			}
		}
	}
}

func affinityAdmits(a *corev1.Affinity, labels map[string]string) bool {
	if a == nil {
		return true
	}
	for _, term := range a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		ok := true
		for _, e := range term.MatchExpressions {
			v, has := labels[e.Key]
			switch e.Operator {
			case corev1.NodeSelectorOpExists:
				ok = ok && has
			case corev1.NodeSelectorOpIn:
				in := false
				for _, x := range e.Values {
					in = in || (has && v == x)
				}
				ok = ok && in
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func TestPreferredAffinity(t *testing.T) {
	for _, g := range []bool{false, true} {
		for _, iso := range []bool{false, true} {
			for _, enc := range []bool{false, true} {
				req := types.Pod{GDPRRequired: g, IsolationRequired: iso, EncryptionRequired: enc}
				hard, soft := privacyAffinity(req, false), privacyAffinity(req, true)
				if (hard == nil) != (soft == nil) {
					t.Fatalf("%+v: required %v, preferred %v", req, hard, soft)
				}
				if hard == nil {
					continue
				}
				if soft.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
					t.Fatalf("%+v: the preferred form must not require anything", req)
				}
				terms := hard.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
				prefs := soft.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
				if len(prefs) != len(terms) {
					t.Fatalf("%+v: %d preferences for %d terms", req, len(prefs), len(terms))
				}
				for i := range terms {
					if prefs[i].Weight != 100 || fmt.Sprint(prefs[i].Preference) != fmt.Sprint(terms[i]) {
						t.Fatalf("%+v: preference %d is %+v, want weight 100 and %+v", req, i, prefs[i], terms[i])
					}
				}
			}
		}
	}
}

func TestKarmadaLabels(t *testing.T) {
	const n, seed, ratio = 200, 101, 0.5
	plain, labelled := fake.NewSimpleClientset(), fake.NewSimpleClientset()
	_, reqs, _ := createBenchPods(context.Background(), plain, "ns", n, 4, "", "app", 0, 0, seed, ratio, true, false, false)
	_, reqsK, _ := createBenchPods(context.Background(), labelled, "ns", n, 4, "", "app", 0, 0, seed, ratio, true, false, true)
	if len(reqs) != len(reqsK) || len(reqs) == 0 {
		t.Fatalf("%d pods with requirements without the label, %d with it", len(reqs), len(reqsK))
	}
	pods, _ := labelled.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{})
	for _, p := range pods.Items {
		if want := reqsK[p.Name].GDPRRequired; (p.Labels["residency"] == "required") != want {
			t.Fatalf("pod %s: label %q, residency requirement %v", p.Name, p.Labels["residency"], want)
		}
	}
	plainPods, _ := plain.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{})
	for _, p := range plainPods.Items {
		if _, ok := p.Labels["residency"]; ok {
			t.Fatalf("pod %s: labelled outside Karmada mode", p.Name)
		}
	}
}

func TestPlacementCheckAddsMembers(t *testing.T) {
	var pc placementCheck
	pc.add(placementCheck{resid: 1, iso: 2, enc: 3, withAffinity: 4, withoutAffinity: 5, violatingWithAffinity: 6, unconstrainedOnCompliant: 7, unconstrainedOnCompliantCPU: 8})
	pc.add(placementCheck{resid: 10, iso: 20, enc: 30, withAffinity: 40, withoutAffinity: 50, violatingWithAffinity: 60, unconstrainedOnCompliant: 70, unconstrainedOnCompliantCPU: 80})
	want := placementCheck{resid: 11, iso: 22, enc: 33, withAffinity: 44, withoutAffinity: 55, violatingWithAffinity: 66, unconstrainedOnCompliant: 77, unconstrainedOnCompliantCPU: 88}
	if pc != want {
		t.Fatalf("got %+v, want %+v", pc, want)
	}
}

func TestClaimingPolicy(t *testing.T) {
	for _, c := range []struct {
		a    map[string]string
		want string
	}{
		{map[string]string{"propagationpolicy.karmada.io/namespace": "tenant-a", "propagationpolicy.karmada.io/name": "tenant-spread"}, "PropagationPolicy tenant-a/tenant-spread"},
		{map[string]string{"clusterpropagationpolicy.karmada.io/name": "residency"}, "ClusterPropagationPolicy residency"},
		{nil, "none"},
	} {
		if got := claimingPolicy(c.a); got != c.want {
			t.Fatalf("%v: got %q, want %q", c.a, got, c.want)
		}
	}
}

func TestParseMembers(t *testing.T) {
	if m, err := parseMembers("", 1, 1); err != nil || m != nil {
		t.Fatalf("empty: %v %v", m, err)
	}
	for _, bad := range []string{"m-eu", "=path", "m-eu="} {
		if _, err := parseMembers(bad, 1, 1); err == nil {
			t.Fatalf("%q: expected an error", bad)
		}
	}
}

func TestUnschedulableCondition(t *testing.T) {
	pod := func(conds ...corev1.PodCondition) *corev1.Pod {
		return &corev1.Pod{Status: corev1.PodStatus{Conditions: conds}}
	}
	refused := corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}
	if !unschedulable(pod(refused)) {
		t.Fatal("PodScheduled false with reason Unschedulable is a refusal")
	}
	if unschedulable(pod()) || unschedulable(pod(corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue})) {
		t.Fatal("a pod not yet tried, or scheduled, is not refused")
	}
	other := refused
	other.Reason = "SchedulerError"
	if unschedulable(pod(other)) {
		t.Fatal("only the Unschedulable reason is a refusal")
	}
}

func TestFederationDraws(t *testing.T) {
	const n, ratio = 2000, 0.5
	members := []string{"m-eu", "m-us", "m-asia", "m-other"}
	sumCmp, sumFed := 0, 0
	for seed := int64(101); seed <= 105; seed++ {
		_, reqs, _ := createBenchPods(context.Background(), fake.NewSimpleClientset(), "tenant-a", n, 4, "", "app", 0, 0, seed, ratio, true, false, true)
		clients := map[string]kubernetes.Interface{}
		for _, m := range members {
			clients[m] = fake.NewSimpleClientset()
		}
		infos, _, err := generators.CreatePods(fake.NewSimpleClientset(), generators.PodGenConfig{
			Namespace: "gated-bench", Count: n, ClusterCount: 1, Profile: "synthetic", PrivacyRatio: ratio, Seed: seed,
			HomeClusters: members, ClusterClients: clients,
		})
		if err != nil {
			t.Fatal(err)
		}
		fed, same := 0, 0
		for i, info := range infos {
			if info.GDPRRequired {
				fed++
			}
			r := reqs[fmt.Sprintf("compare-pod-%d", i)]
			if r.GDPRRequired == info.GDPRRequired && r.IsolationRequired == info.IsolationRequired && r.EncryptionRequired == info.EncryptionRequired {
				same++
			}
		}
		t.Logf("seed %d: residency-bound pods %d (comparison harness), %d (federation); requirements equal for %d of %d pods", seed, len(reqs), fed, same, n)
		sumCmp += len(reqs)
		sumFed += fed
	}
	t.Logf("seeds 101-105: %d and %d residency-bound pods", sumCmp, sumFed)
}
