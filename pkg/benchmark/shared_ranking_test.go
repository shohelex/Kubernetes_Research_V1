package benchmark

import (
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func TestSharedRankingTake(t *testing.T) {
	a := types.Node{Name: "a", CPUCores: 2, Memory: 64, Storage: 100}
	b := types.Node{Name: "b", CPUCores: 2, Memory: 64, Storage: 100}
	s := &sharedRanking{list: []scheduler.RankedNode{{Node: a, Score: 2}, {Node: b, Score: 1}}}
	tr := scheduler.NewNodeResourceTracker()
	pod := types.Pod{CPU: 1000}
	rejects, gateRejects := 0, 0

	for i, want := range []string{"a", "a", "b", "b"} {
		got, ok := s.take(tr, pod, &rejects, &gateRejects)
		if !ok || got.Name != want {
			t.Fatalf("pod %d: got %q (ok=%v), want %q", i+1, got.Name, ok, want)
		}
	}
	if rejects != 1 || len(s.list) != 1 || s.list[0].Node.Name != "b" {
		t.Fatalf("after node a filled: rejects=%d list=%v", rejects, s.list)
	}
	if _, ok := s.take(tr, pod, &rejects, &gateRejects); ok || len(s.list) != 0 {
		t.Fatal("with every node full the list must run out")
	}
}

func TestRecheckDropsRelabelledNode(t *testing.T) {
	a := types.Node{Name: "a", CPUCores: 8, Memory: 64, Storage: 100, Region: types.RegionEurope}
	b := types.Node{Name: "b", CPUCores: 8, Memory: 64, Storage: 100, Region: types.RegionEurope}
	pod := types.Pod{CPU: 1000, GDPRRequired: true}

	defer func() { scheduler.Live = nil }()
	for _, recheck := range []bool{false, true} {
		scheduler.Live = scheduler.NewLiveLabels([]types.Node{a, b})
		moved := scheduler.PrivacyLabelsOf(a)
		moved.Region = types.RegionUS
		scheduler.Live.Set("a", moved)

		s := &sharedRanking{list: []scheduler.RankedNode{{Node: a, Score: 2}, {Node: b, Score: 1}}, recheckGates: recheck}
		rejects, gateRejects := 0, 0
		got, ok := s.take(scheduler.NewNodeResourceTracker(), pod, &rejects, &gateRejects)
		if !ok {
			t.Fatalf("recheck=%v: no node taken", recheck)
		}
		violated := len(scheduler.CheckViolations(pod, scheduler.CurrentNode(got))) > 0
		switch {
		case !recheck && (got.Name != "a" || !violated || gateRejects != 0):
			t.Fatalf("without the re-check the stale node is booked: got %s, violated=%v, gateRejects=%d", got.Name, violated, gateRejects)
		case recheck && (got.Name != "b" || violated || gateRejects != 1):
			t.Fatalf("with the re-check the stale node is dropped: got %s, violated=%v, gateRejects=%d", got.Name, violated, gateRejects)
		}
	}
}

func TestReuseNeedsSameHomeAndOrigin(t *testing.T) {
	n := types.Node{Name: "a", CPUCores: 8, Memory: 64, Storage: 100}
	base := types.Pod{CPU: 2000, Memory: 1024, Latency: 50, HomeCluster: "eu-1", DataRegion: types.RegionEurope}
	s := &sharedRanking{list: []scheduler.RankedNode{{Node: n, Score: 1}}, forPod: &base, priority: "balanced"}

	same := base
	same.CPU = 1000
	if !s.reusableFor(same, "balanced") {
		t.Fatal("a smaller pod from the same home and origin must reuse the ranking")
	}
	otherHome := same
	otherHome.HomeCluster = "us-1"
	if s.reusableFor(otherHome, "balanced") {
		t.Fatal("a pod from another home cluster must not reuse a ranking scored for this one")
	}
	otherOrigin := same
	otherOrigin.DataRegion = types.RegionUS
	if s.reusableFor(otherOrigin, "balanced") {
		t.Fatal("a pod whose data comes from another region must not reuse the ranking")
	}
	if s.reusableFor(same, "privacy") {
		t.Fatal("a pod under another priority must not reuse the ranking")
	}
	bigger := base
	bigger.CPU = 4000
	if s.reusableFor(bigger, "balanced") {
		t.Fatal("a pod that asks for more must not reuse the ranking")
	}

	one := types.Pod{CPU: 2000, Memory: 1024, Latency: 50}
	s1 := &sharedRanking{list: s.list, forPod: &one, priority: "balanced"}
	small := one
	small.CPU = 1000
	if !s1.reusableFor(small, "balanced") {
		t.Fatal("in one cluster reuse must work as before")
	}
	if (&sharedRanking{forPod: &one, priority: "balanced"}).reusableFor(small, "balanced") {
		t.Fatal("an empty list is not reusable")
	}
}

func TestRankingSetClasses(t *testing.T) {
	one := newRankingSet(false, false)
	if one.pick("balanced", types.Pod{GDPRRequired: true}) != one.pick("privacy", types.Pod{HomeCluster: "c1"}) {
		t.Fatal("without classes every pod must share the one ranking")
	}
	if one.nextID() != 1 || one.nextID() != 2 {
		t.Fatal("ranking numbers must run 1, 2, ...")
	}

	rs := newRankingSet(true, true)
	g1 := rs.pick("balanced", types.Pod{GDPRRequired: true, CPU: 1000})
	g2 := rs.pick("balanced", types.Pod{GDPRRequired: true, CPU: 8000})
	if g1 != g2 {
		t.Fatal("two pods of one class must share its ranking, whatever their size")
	}
	for name, other := range map[string]*sharedRanking{
		"no requirements":  rs.pick("balanced", types.Pod{}),
		"isolation too":    rs.pick("balanced", types.Pod{GDPRRequired: true, IsolationRequired: true}),
		"another priority": rs.pick("privacy", types.Pod{GDPRRequired: true}),
		"another home":     rs.pick("balanced", types.Pod{GDPRRequired: true, HomeCluster: "c1"}),
		"another origin":   rs.pick("balanced", types.Pod{GDPRRequired: true, DataRegion: types.RegionUS}),
	} {
		if other == g1 {
			t.Fatalf("a pod with %s must not share the ranking", name)
		}
	}
	if !g1.recheckGates {
		t.Fatal("every class's ranking must carry the bind-time re-check")
	}
	seen := map[uint64]bool{}
	for i := 0; i < 5; i++ {
		id := rs.nextID()
		if seen[id] {
			t.Fatalf("ranking number %d given twice", id)
		}
		seen[id] = true
	}
}

func TestDefaultRankingsArePerClass(t *testing.T) {
	rs := newRankingSet(false, DefaultReuseClasses)
	bound := types.Pod{GDPRRequired: true, CPU: 1000}
	free := types.Pod{CPU: 500}
	eu := types.Node{Name: "eu", Region: types.RegionEurope}
	s := rs.pick("balanced", bound)
	s.list, s.forPod, s.priority = []scheduler.RankedNode{{Node: eu}}, &bound, "balanced"
	if other := rs.pick("balanced", free); other == s || other.reusableFor(free, "balanced") {
		t.Fatal("a pod without the requirement must get a ranking of its own, not the residency-bound one")
	}
	if got := rs.pick("balanced", bound); got != s || !got.reusableFor(bound, "balanced") {
		t.Fatal("the next residency-bound pod must still find its class's ranking")
	}
}

func TestDefaultRankingsRecheckGates(t *testing.T) {
	a := types.Node{Name: "a", CPUCores: 8, Memory: 64, Storage: 100, Region: types.RegionEurope}
	b := types.Node{Name: "b", CPUCores: 8, Memory: 64, Storage: 100, Region: types.RegionEurope}
	pod := types.Pod{CPU: 1000, GDPRRequired: true}
	defer func() { scheduler.Live = nil }()
	scheduler.Live = scheduler.NewLiveLabels([]types.Node{a, b})
	moved := scheduler.PrivacyLabelsOf(a)
	moved.Region = types.RegionUS
	scheduler.Live.Set("a", moved)

	s := newRankingSet(DefaultRecheckGates, DefaultReuseClasses).pick("balanced", pod)
	s.list = []scheduler.RankedNode{{Node: a, Score: 2}, {Node: b, Score: 1}}
	rejects, gateRejects := 0, 0
	if got, ok := s.take(scheduler.NewNodeResourceTracker(), pod, &rejects, &gateRejects); !ok || got.Name != "b" || gateRejects != 1 {
		t.Fatalf("got %s (ok=%v, gateRejects=%d); want b, with a dropped", got.Name, ok, gateRejects)
	}
}

func TestRecheckWithoutGates(t *testing.T) {
	us := types.Node{Name: "us", CPUCores: 8, Memory: 64, Storage: 100, Region: types.RegionUS}
	pod := types.Pod{CPU: 1000, GDPRRequired: true}
	defer func() { scheduler.PrivacyFiltersEnabled = true }()
	scheduler.PrivacyFiltersEnabled = false

	s := newRankingSet(DefaultRecheckGates, DefaultReuseClasses).pick("balanced", pod)
	s.list = []scheduler.RankedNode{{Node: us, Score: 1}}
	rejects, gateRejects := 0, 0
	if got, ok := s.take(scheduler.NewNodeResourceTracker(), pod, &rejects, &gateRejects); !ok || got.Name != "us" || gateRejects != 0 {
		t.Fatalf("got %s (ok=%v, gateRejects=%d); want us booked with the gates off", got.Name, ok, gateRejects)
	}
}
