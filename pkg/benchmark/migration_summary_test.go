package benchmark

import "testing"

func TestMigrationSummary(t *testing.T) {
	lats := []PodLatency{
		{SourceCluster: "cluster1", TargetCluster: "cluster0", Migrated: true, ResidencyRequired: true, Bound: true},
		{SourceCluster: "cluster1", TargetCluster: "cluster0", Migrated: true, ResidencyRequired: true, Bound: true},
		{SourceCluster: "cluster1", TargetCluster: "cluster1", Bound: true},
		{SourceCluster: "cluster1", ResidencyRequired: true},
		{SourceCluster: "cluster0", TargetCluster: "cluster0", ResidencyRequired: true, Bound: true},
		{SourceCluster: "cluster0", TargetCluster: "cluster0", Bound: true},
	}
	ms := buildMigrationSummary(lats, 2, 5)
	want := map[string]HomeClusterStats{
		"cluster1": {Pods: 3, Migrated: 2, ResidencyPods: 2, ResidencyMigrated: 2},
		"cluster0": {Pods: 2, Migrated: 0, ResidencyPods: 1, ResidencyMigrated: 0},
	}
	for home, w := range want {
		got := ms.PerHomeCluster[home]
		if got.Pods != w.Pods || got.Migrated != w.Migrated || got.ResidencyPods != w.ResidencyPods || got.ResidencyMigrated != w.ResidencyMigrated {
			t.Fatalf("%s: got %+v, want pods %d migrated %d residency %d residency migrated %d", home, got,
				w.Pods, w.Migrated, w.ResidencyPods, w.ResidencyMigrated)
		}
	}
}
