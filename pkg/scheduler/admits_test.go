package scheduler

import (
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func TestReserveUtilizationLimit(t *testing.T) {
	node := types.Node{Name: "n", CPUCores: 10, Memory: 100, Storage: 100}
	pod := types.Pod{CPU: 1000, Memory: 1024, Storage: 1}
	tr := NewNodeResourceTracker()

	for i := 0; i < 9; i++ {
		if !tr.Reserve("n", pod, node) {
			t.Fatalf("pod %d refused at %d%% CPU", i+1, i*10)
		}
	}

	if !tr.Admits("n", pod, node) || !tr.Reserve("n", pod, node) {
		t.Fatal("a node at exactly 90 % must still take a pod that fits")
	}

	if tr.Admits("n", pod, node) {
		t.Fatal("a full node must not take a pod")
	}

	tr.Reset()
	big := types.Pod{CPU: 9500}
	if !tr.Reserve("n", big, node) {
		t.Fatal("9.5 cores fit an empty 10-core node")
	}

	if tr.Admits("n", types.Pod{CPU: 100}, node) {
		t.Fatal("a node above 90 % utilisation must not take a pod")
	}
}

func TestReleaseUndoesReserve(t *testing.T) {
	node := types.Node{Name: "n", CPUCores: 4, Memory: 8, Storage: 10}
	pod := types.Pod{CPU: 4000, Memory: 8192}
	tr := NewNodeResourceTracker()
	if !tr.Reserve("n", pod, node) || tr.Admits("n", pod, node) {
		t.Fatal("reserve should fill the node")
	}
	tr.Release("n", pod)
	if !tr.Admits("n", pod, node) {
		t.Fatal("release should free the node")
	}
}

func TestReserveFitOnly(t *testing.T) {
	saved := UtilizationLimit
	UtilizationLimit = 1.0
	t.Cleanup(func() { UtilizationLimit = saved })

	node := types.Node{Name: "n", CPUCores: 10, Memory: 100, Storage: 100}
	tr := NewNodeResourceTracker()
	if !tr.Reserve("n", types.Pod{CPU: 9500}, node) {
		t.Fatal("9.5 cores fit an empty 10-core node")
	}

	if !tr.Admits("n", types.Pod{CPU: 500}, node) || !tr.Reserve("n", types.Pod{CPU: 500}, node) {
		t.Fatal("at limit 1.0 a node at 95 % must take a pod that fits")
	}

	if tr.Admits("n", types.Pod{CPU: 1}, node) {
		t.Fatal("a full node must not take a pod")
	}
}
