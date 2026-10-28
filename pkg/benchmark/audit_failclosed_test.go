package benchmark

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/auditlog"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	corev1 "k8s.io/api/core/v1"
)

func runWithFailingLog(t *testing.T, mode auditlog.Mode) (RunMetrics, int) {
	t.Helper()
	infos := testPods(40)
	client := fakeCluster(t, infos)
	dir := t.TempDir()
	path := filepath.Join(dir, "decisions.jsonl")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a, err := os.Create(path + ".anchor")
	if err != nil {
		t.Fatal(err)
	}
	w, err := auditlog.NewWriter(f, a, mode, 50)
	if err != nil {
		t.Fatal(err)
	}
	podCh := make(chan generators.PodInfo, len(infos))
	for _, pi := range infos {
		podCh <- pi
	}
	close(podCh)
	weights, _ := scheduler.GetPerformanceWeights("general", 0.10)
	cfg := Config{Pods: len(infos), Schedulers: 4, Priority: "balanced", audit: w}
	rm := executeRun(0, client, nil, scheduler.NewNodeResourceTracker(), testNodes(), weights, cfg, podCh)
	w.Close()
	pods, _ := client.Tracker().List(corev1.SchemeGroupVersion.WithResource("pods"),
		corev1.SchemeGroupVersion.WithKind("Pod"), "gated-bench")
	bound := 0
	for _, p := range pods.(*corev1.PodList).Items {
		if p.Spec.NodeName != "" {
			bound++
		}
	}
	return rm, bound
}

func TestSyncDecisionLogFailsClosed(t *testing.T) {
	rm, bound := runWithFailingLog(t, auditlog.Sync)
	if bound != 0 || rm.Scheduled != 0 || rm.AuditErrors == 0 {
		t.Fatalf("sync with a failing log: %d pods bound, %d scheduled, %d log errors; want none bound and errors counted",
			bound, rm.Scheduled, rm.AuditErrors)
	}
	rm, bound = runWithFailingLog(t, auditlog.Async)
	if bound == 0 || rm.Scheduled != bound {
		t.Fatalf("async with a failing log: %d pods bound, %d scheduled; want the pods placed", bound, rm.Scheduled)
	}
}
