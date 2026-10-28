package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/auditlog"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func fakeCluster(t *testing.T, infos []generators.PodInfo) *fake.Clientset {
	t.Helper()
	client := fake.NewSimpleClientset()
	podsGVR := corev1.SchemeGroupVersion.WithResource("pods")
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := action.(k8stesting.CreateAction)
		if !ok || ca.GetSubresource() != "binding" {
			return false, nil, nil
		}
		b := ca.GetObject().(*corev1.Binding)
		obj, err := client.Tracker().Get(podsGVR, b.Namespace, b.Name)
		if err != nil {
			return true, nil, err
		}
		p := obj.(*corev1.Pod).DeepCopy()
		p.Spec.NodeName = b.Target.Name
		return true, b, client.Tracker().Update(podsGVR, p, b.Namespace)
	})
	for _, pi := range infos {
		if err := client.Tracker().Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pi.Name, Namespace: pi.Namespace}}); err != nil {
			t.Fatal(err)
		}
	}
	return client
}

func testNodes() []types.Node {
	enc := 50.0
	regions := []types.Region{types.RegionEurope, types.RegionUS, types.RegionAsia, types.RegionOther}
	isos := []types.Isolation{types.IsolationNone, types.IsolationNetworkPolicy, types.IsolationDedicated}
	var nodes []types.Node
	for i := 0; i < 40; i++ {
		n := types.Node{Name: fmt.Sprintf("n%d", i), CPUCores: 128, Memory: 256, Storage: 2000, Bandwidth: 1000,
			IOSpeed: 2000, Ping: 10, LCache: 8, Exposure: types.ExposurePrivate, Region: regions[i%4],
			Isolation: isos[i%3], Cluster: "cluster0"}
		if i%2 == 0 {
			n.EncryptedStorage = &enc
		}
		nodes = append(nodes, n)
	}
	return nodes
}

func testPods(n int) []generators.PodInfo {
	var infos []generators.PodInfo
	for i := 0; i < n; i++ {
		pi := generators.PodInfo{Name: fmt.Sprintf("p%d", i), Namespace: "gated-bench", SizeName: "small",
			CPUMilli: 4000, MemoryMB: 5120, StorageGB: 2, Bandwidth: 10, IOSpeed: 200, Latency: 100}
		if i%2 == 0 {
			pi.GDPRRequired, pi.IsolationRequired, pi.EncryptionRequired = true, i%4 == 0, i%3 == 0
		}
		infos = append(infos, pi)
	}
	return infos
}

func runWithLog(t *testing.T, mode auditlog.Mode, gates bool) (RunMetrics, string, *fake.Clientset) {
	t.Helper()
	infos := testPods(200)
	client := fakeCluster(t, infos)
	path := filepath.Join(t.TempDir(), "decisions.jsonl")
	w, err := auditlog.Open(path, mode, 50)
	if err != nil {
		t.Fatal(err)
	}
	scheduler.PrivacyFiltersEnabled = gates
	defer func() { scheduler.PrivacyFiltersEnabled = true }()
	podCh := make(chan generators.PodInfo, len(infos))
	for _, pi := range infos {
		podCh <- pi
	}
	close(podCh)
	weights, _ := scheduler.GetPerformanceWeights("general", 0.10)
	cfg := Config{Pods: len(infos), Schedulers: 4, Priority: "balanced", audit: w}
	rm := executeRun(0, client, nil, scheduler.NewNodeResourceTracker(), testNodes(), weights, cfg, podCh)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return rm, path, client
}

func verifyRun(t *testing.T, path string, client *fake.Clientset) auditlog.Report {
	t.Helper()
	lf, _ := os.Open(path)
	defer lf.Close()
	af, _ := os.Open(path + ".anchor")
	defer af.Close()
	rep, entries := auditlog.Verify(lf, af, DecisionCheck)
	pods, _ := client.Tracker().List(corev1.SchemeGroupVersion.WithResource("pods"),
		corev1.SchemeGroupVersion.WithKind("Pod"), "gated-bench")
	placements := map[string]string{}
	for _, p := range pods.(*corev1.PodList).Items {
		if p.Spec.NodeName != "" {
			placements[p.Namespace+"/"+p.Name] = p.Spec.NodeName
		}
	}
	auditlog.CrossCheck(&rep, entries, placements)
	return rep
}

func TestHarnessDecisionLogVerifies(t *testing.T) {
	for _, mode := range []auditlog.Mode{auditlog.Sync, auditlog.Async} {
		rm, path, client := runWithLog(t, mode, true)
		rep := verifyRun(t, path, client)
		if rm.Scheduled == 0 || rep.Decisions != rm.Scheduled+rm.Failed || !rep.OK() {
			t.Fatalf("%s: scheduled %d failed %d; report %+v", mode, rm.Scheduled, rm.Failed, rep)
		}
	}
}

func TestDecisionLogViolations(t *testing.T) {
	rm, path, client := runWithLog(t, auditlog.Sync, false)
	rep := verifyRun(t, path, client)
	if !rep.ChainOK || len(rep.BadDecisions) == 0 {
		t.Fatalf("gates off: expected violations in the log; scheduled %d, report %+v", rm.Scheduled, rep)
	}

	if rm.Violations == nil || len(rep.BadDecisions) != rm.Violations.PodsViolating {
		t.Fatalf("log shows %d violating decisions, harness counted %+v", len(rep.BadDecisions), rm.Violations)
	}
}
