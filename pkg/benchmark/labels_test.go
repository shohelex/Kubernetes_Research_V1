package benchmark

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestLabelChurnRestore(t *testing.T) {
	var objs []corev1.Node
	for i := 0; i < 6; i++ {
		region := "Europe"
		if i >= 4 {
			region = "Asia"
		}
		objs = append(objs, corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   fmt.Sprintf("n%d", i),
			Labels: map[string]string{"type": "kwok", "region": region, "isolation": "dedicated"},
		}})
	}
	client := fake.NewSimpleClientset(&objs[0], &objs[1], &objs[2], &objs[3], &objs[4], &objs[5])
	var nodes []types.Node
	for i := range objs {
		n, _ := generators.NodeFromK8s(&objs[i])
		nodes = append(nodes, n)
	}
	live := scheduler.NewLiveLabels(nodes)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchNodeLabels(ctx, client, live)

	churn := newLabelChurn(client, live, 5*time.Millisecond, "random", nil, nodes, 1)
	churnCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { churn.run(churnCtx); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		moved := 0
		for _, n := range nodes[:4] {
			if p, _ := live.Get(n.Name); p.Region == types.RegionUS {
				moved++
			}
		}
		if moved == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 4 European nodes moved", moved)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	<-done

	for _, n := range nodes[4:] {
		if p, _ := live.Get(n.Name); p.Region != types.RegionAsia {
			t.Fatalf("%s was not in Europe and must not move, now %s", n.Name, p.Region)
		}
	}
	got, err := churn.restore(5 * time.Second)
	if err != nil || got != 4 {
		t.Fatalf("restore: %d nodes, err %v; want 4", got, err)
	}
	for _, n := range nodes[:4] {
		if p, _ := live.Get(n.Name); p.Region != types.RegionEurope {
			t.Fatalf("%s not put back in Europe: %s", n.Name, p.Region)
		}
		k, _ := client.CoreV1().Nodes().Get(context.Background(), n.Name, metav1.GetOptions{})
		if k.Labels["region"] != "Europe" {
			t.Fatalf("%s label on the API server: %q", n.Name, k.Labels["region"])
		}
	}
}

func TestLabelChurnTop(t *testing.T) {
	a := types.Node{Name: "a", Region: types.RegionAsia}
	b := types.Node{Name: "b", Region: types.RegionEurope}
	c := types.Node{Name: "c", Region: types.RegionEurope}
	live := scheduler.NewLiveLabels([]types.Node{a, b, c})
	rs := newRankingSet(false, false)
	rs.single.list = []scheduler.RankedNode{{Node: a}, {Node: c}, {Node: b}}
	churn := newLabelChurn(fake.NewSimpleClientset(), live, time.Second, "top", rs, []types.Node{a, b, c}, 1)
	if name, ok := churn.pick(); !ok || name != "c" {
		t.Fatalf("pick = %q, %v; want c, the first European node of the ranking", name, ok)
	}
}

func TestLabelChurnTopClass(t *testing.T) {
	a := types.Node{Name: "a", Region: types.RegionEurope}
	b := types.Node{Name: "b", Region: types.RegionEurope}
	c := types.Node{Name: "c", Region: types.RegionEurope}
	nodes := []types.Node{a, b, c}
	live := scheduler.NewLiveLabels(nodes)
	rs := newRankingSet(true, true)
	churn := newLabelChurn(fake.NewSimpleClientset(), live, time.Second, "top", rs, nodes, 1)

	free := rs.pick("balanced", types.Pod{})
	free.list = []scheduler.RankedNode{{Node: a}, {Node: b}, {Node: c}}
	if name, ok := churn.pick(); !ok {
		t.Fatalf("pick = %q, %v; want a random European node before any residency-bound pod", name, ok)
	}

	bound := rs.pick("balanced", types.Pod{GDPRRequired: true})
	bound.list = []scheduler.RankedNode{{Node: c}, {Node: b}}
	rs.pick("balanced", types.Pod{})
	if name, ok := churn.pick(); !ok || name != "c" {
		t.Fatalf("pick = %q, %v; want c, the first node of the residency-bound class's ranking", name, ok)
	}
}
