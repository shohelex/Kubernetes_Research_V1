package scheduler

import (
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func TestUnconstrainedOnCompliant(t *testing.T) {
	eu := types.Node{Name: "eu", Region: "Europe"}
	us := types.Node{Name: "us", Region: "United States"}
	var a, b ViolationCounter
	a.Record(types.Pod{Name: "free-eu", CPU: 4000}, eu)
	a.Record(types.Pod{Name: "bound-eu", CPU: 8000, GDPRRequired: true}, eu)
	b.Record(types.Pod{Name: "free-us", CPU: 16000}, us)
	b.Record(types.Pod{Name: "free-eu2", CPU: 2000}, eu)
	a.Merge(b)
	r := a.Report()
	if r.UnconstrainedOnCompliant != 2 || r.UnconstrainedOnCompliantCPU != 6000 {
		t.Fatalf("got %d pods, %d m; want 2 pods, 6000 m", r.UnconstrainedOnCompliant, r.UnconstrainedOnCompliantCPU)
	}
	if r.Residency != 0 {
		t.Fatalf("no residency violation expected, got %d", r.Residency)
	}
}
