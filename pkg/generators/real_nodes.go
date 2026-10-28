package generators

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

const RealNodeLabel = "placement-gates/bench"

type RealNodeConfig struct {
	Seed         int64
	ClusterCount int
	Regions      []string

	RegionMode string
}

func DefaultRealNodeConfig(seed int64, clusterCount int) RealNodeConfig {
	return RealNodeConfig{
		Seed:         seed,
		ClusterCount: clusterCount,
		Regions:      []string{"Europe", "United States", "Asia", "Other"},
		RegionMode:   "weighted",
	}
}

func regionLabelValue(region string) string {
	return strings.ReplaceAll(region, " ", "-")
}

func regionFromLabel(v string) types.Region {
	return types.Region(strings.ReplaceAll(v, "-", " "))
}

func allocatableCPUCores(n *corev1.Node) uint32 {
	q := n.Status.Allocatable[corev1.ResourceCPU]
	m := q.MilliValue()
	if m <= 0 {
		return 1
	}
	cores := m / 1000
	if cores < 1 {
		cores = 1
	}
	return uint32(cores)
}

func allocatableMemoryGB(n *corev1.Node) uint32 {
	q := n.Status.Allocatable[corev1.ResourceMemory]
	gb := q.Value() / (1 << 30)
	if gb < 1 {
		gb = 1
	}
	return uint32(gb)
}

func allocatableStorageGB(n *corev1.Node) uint32 {
	q := n.Status.Allocatable[corev1.ResourceEphemeralStorage]
	gb := q.Value() / (1 << 30)
	if gb < 1 {
		gb = 1
	}
	return uint32(gb)
}

func isSchedulable(n *corev1.Node) bool {
	if n.Spec.Unschedulable {
		return false
	}
	for _, t := range n.Spec.Taints {
		if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	return true
}

func LabelRealNodes(clientset kubernetes.Interface, cfg RealNodeConfig) (int, error) {
	ctx := context.Background()
	list, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("listing nodes: %w", err)
	}

	var targets []corev1.Node
	for _, n := range list.Items {
		if isSchedulable(&n) {
			targets = append(targets, n)
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Name < targets[j].Name })
	if len(targets) == 0 {
		return 0, fmt.Errorf("no schedulable nodes found")
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	clusterCount := cfg.ClusterCount
	if clusterCount < 1 {
		clusterCount = 1
	}
	regions := cfg.Regions
	if len(regions) == 0 {
		regions = []string{"Europe"}
	}

	for i := range targets {
		n := &targets[i]

		cpu := allocatableCPUCores(n)
		mem := allocatableMemoryGB(n)
		stor := allocatableStorageGB(n)

		var region string
		if cfg.RegionMode == "uniform" {
			region = regions[i%len(regions)]
		} else {
			region = "Other"
			rv := rng.Float64()
			for _, rw := range regionWeights {
				if rv < rw.Weight {
					region = rw.Name
					break
				}
			}
		}

		exposure := "private"
		if rng.Float64() >= privateRatio {
			exposure = "public"
		}

		isolation := "none"
		iv := rng.Float64()
		for _, iw := range isolationWeights {
			if iv < iw.Weight {
				isolation = iw.Name
				break
			}
		}

		bandwidth := 1000 + rng.Intn(9001)
		latency := 1 + rng.Intn(20)
		lcache := 8 + rng.Intn(57)
		iospeed := 200 + rng.Intn(1801)

		labels := map[string]string{
			RealNodeLabel:  "true",
			"region":       regionLabelValue(region),
			"bandwidth":    strconv.Itoa(bandwidth),
			"latency":      strconv.Itoa(latency),
			"iospeed":      strconv.Itoa(iospeed),
			"cpu_capacity": strconv.FormatUint(uint64(cpu), 10),
			"memory_gb":    strconv.FormatUint(uint64(mem), 10),
			"storage":      strconv.FormatUint(uint64(stor), 10),
			"l_cache":      strconv.Itoa(lcache),
			"exposure":     exposure,
			"isolation":    isolation,
			"cluster":      fmt.Sprintf("cluster%d", i%clusterCount),
		}

		if rng.Float64() < 0.9 {
			labels["encrypted_storage"] = strconv.Itoa(rng.Intn(101))
		}
		if rng.Float64() < 0.9 {
			labels["encrypted_traffic"] = fmt.Sprintf("%d-%d-0.01-%d-%d",
				1+rng.Intn(10), iospeed, 10+rng.Intn(41), 1+rng.Intn(10))
		}

		patch := map[string]interface{}{
			"metadata": map[string]interface{}{"labels": labels},
		}
		body, err := json.Marshal(patch)
		if err != nil {
			return 0, fmt.Errorf("marshalling label patch: %w", err)
		}
		if _, err := clientset.CoreV1().Nodes().Patch(ctx, n.Name,
			k8stypes.MergePatchType, body, metav1.PatchOptions{}); err != nil {
			return 0, fmt.Errorf("labelling node %s: %w", n.Name, err)
		}

		fmt.Printf("    %-12s cpu=%-3d mem=%-4d stor=%-4d region=%-14s iso=%-14s enc=%v\n",
			n.Name, cpu, mem, stor, region, isolation, labels["encrypted_storage"] != "")
	}

	return len(targets), nil
}

func ReadRealNodes(clientset kubernetes.Interface) ([]types.Node, error) {
	ctx := context.Background()
	list, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: RealNodeLabel + "=true",
	})
	if err != nil {
		return nil, fmt.Errorf("listing real nodes: %w", err)
	}

	var nodes []types.Node
	for i := range list.Items {
		k := &list.Items[i]
		labels := k.Labels

		exposure := types.Exposure(labels["exposure"])
		if exposure == "" {
			exposure = types.ExposurePrivate
		}
		isolation := types.Isolation(labels["isolation"])
		if isolation == "" {
			isolation = types.IsolationNone
		}
		region := regionFromLabel(labels["region"])
		if region == "" {
			region = types.RegionOther
		}
		cluster := labels["cluster"]
		if cluster == "" {
			cluster = "cluster0"
		}

		var encStorage *float64
		if v, ok := labels["encrypted_storage"]; ok && v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				encStorage = &f
			}
		}
		var encTraffic *types.TrafficEncryption
		if v, ok := labels["encrypted_traffic"]; ok && v != "" {
			encTraffic = parseTrafficEncryption(v)
		}

		nodes = append(nodes, types.Node{
			Name: k.Name,

			CPUCores:         allocatableCPUCores(k),
			Memory:           allocatableMemoryGB(k),
			Storage:          allocatableStorageGB(k),
			Bandwidth:        parseUint32(labels["bandwidth"], 1000),
			IOSpeed:          parseUint32(labels["iospeed"], 500),
			Ping:             parseUint32(labels["latency"], 5),
			LCache:           parseUint32(labels["l_cache"], 16),
			Exposure:         exposure,
			Isolation:        isolation,
			EncryptedStorage: encStorage,
			EncryptedTraffic: encTraffic,
			Region:           region,
			Cluster:          cluster,
		})
	}

	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return nodes, nil
}

func NodeAllocatableSummary(clientset kubernetes.Interface) (cores, memGB, podSlots int, err error) {
	ctx := context.Background()
	list, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: RealNodeLabel + "=true",
	})
	if err != nil {
		return 0, 0, 0, err
	}
	for i := range list.Items {
		n := &list.Items[i]
		cores += int(allocatableCPUCores(n))
		memGB += int(allocatableMemoryGB(n))
		p := n.Status.Allocatable[corev1.ResourcePods]
		podSlots += int(p.Value())
	}
	return cores, memGB, podSlots, nil
}
