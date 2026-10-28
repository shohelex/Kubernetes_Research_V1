package generators

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

type NodeSize struct {
	CPU     int
	Memory  int
	Storage int
	IOPS    int
}

var nodeSizes = map[string]NodeSize{
	"medium": {CPU: 64, Memory: 128, Storage: 1000, IOPS: 10000},
	"large":  {CPU: 128, Memory: 256, Storage: 2000, IOPS: 20000},
	"xlarge": {CPU: 256, Memory: 512, Storage: 4000, IOPS: 40000},
}

var nodeSizeNames = []string{"medium", "large", "xlarge"}

var regionWeights = []struct {
	Name   string
	Weight float64
}{
	{"Europe", 0.80},
	{"United States", 0.90},
	{"Asia", 0.97},
	{"Other", 1.00},
}

const privateRatio = 0.7

var isolationWeights = []struct {
	Name   string
	Weight float64
}{
	{"none", 0.05},
	{"namespace", 0.15},
	{"network_policy", 0.40},
	{"dedicated", 0.75},
	{"vm_isolated", 1.00},
}

func CreateKWOKNodes(clientset kubernetes.Interface, count, clusterCount int) error {
	return CreateKWOKNodesNamed(clientset, count, clusterCount, "gated-node")
}

func CreateKWOKNodesNamed(clientset kubernetes.Interface, count, clusterCount int, prefix string) error {
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()
	var failed, existing int
	var firstErr error

	fmt.Printf("  Creating %d KWOK nodes across %d cluster(s)...\n", count, clusterCount)

	for i := 0; i < count; i++ {
		clusterID := i % clusterCount
		sizeName := nodeSizeNames[rng.Intn(len(nodeSizeNames))]
		spec := nodeSizes[sizeName]

		region := "Other"
		rv := rng.Float64()
		for _, rw := range regionWeights {
			if rv < rw.Weight {
				region = rw.Name
				break
			}
		}

		exposure := "public"
		if rng.Float64() < privateRatio {
			exposure = "private"
		}

		isolation := "none"
		iv := rng.Float64()
		for _, iw := range isolationWeights {
			if iv < iw.Weight {
				isolation = iw.Name
				break
			}
		}

		bandwidth := 100 + rng.Intn(9901)
		latency := 1 + rng.Intn(50)
		lcache := 8 + rng.Intn(57)

		labels := map[string]string{
			"kubernetes.io/hostname": fmt.Sprintf("%s-%d", prefix, i),
			"type":                   "kwok",
			"region":                 regionLabelValue(region),
			"bandwidth":              strconv.Itoa(bandwidth),
			"latency":                strconv.Itoa(latency),
			"iospeed":                strconv.Itoa(spec.IOPS),
			"cpu_capacity":           strconv.Itoa(spec.CPU),
			"memory_gb":              strconv.Itoa(spec.Memory),
			"storage":                strconv.Itoa(spec.Storage),
			"l_cache":                strconv.Itoa(lcache),
			"exposure":               exposure,
			"isolation":              isolation,
			"cluster":                fmt.Sprintf("cluster%d", clusterID),
		}

		if rng.Float64() < 0.9 {
			labels["encrypted_storage"] = strconv.Itoa(rng.Intn(101))
		}

		if rng.Float64() < 0.9 {
			labels["encrypted_traffic"] = fmt.Sprintf("%d-%d-0.01-%d-%d",
				1+rng.Intn(10),
				spec.IOPS,
				10+rng.Intn(41),
				1+rng.Intn(10),
			)
		}

		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   fmt.Sprintf("%s-%d", prefix, i),
				Labels: labels,
				Annotations: map[string]string{
					"kwok.x-k8s.io/node": "fake",
				},
			},
			Spec: corev1.NodeSpec{
				Taints: []corev1.Taint{
					{
						Key:    "kwok.x-k8s.io/node",
						Value:  "fake",
						Effect: corev1.TaintEffectNoSchedule,
					},
				},
			},
			Status: corev1.NodeStatus{
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(strconv.Itoa(spec.CPU)),
					corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", spec.Memory)),
					corev1.ResourcePods:   resource.MustParse("110"),
				},
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(strconv.Itoa(spec.CPU)),
					corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", spec.Memory)),
					corev1.ResourcePods:   resource.MustParse("110"),
				},
				Conditions: []corev1.NodeCondition{
					{
						Type:               corev1.NodeReady,
						Status:             corev1.ConditionTrue,
						LastHeartbeatTime:  metav1.Now(),
						LastTransitionTime: metav1.Now(),
						Reason:             "KubeletReady",
						Message:            "kubelet is posting ready status",
					},
				},
			},
		}

		_, err := clientset.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				existing++
			} else {
				failed++
				if firstErr == nil {
					firstErr = err
				}
			}
		}

		if (i+1)%100 == 0 {
			fmt.Printf("    %d/%d nodes created...\n", i+1, count)
		}
	}

	if failed > 0 {
		return fmt.Errorf("%d of %d nodes could not be created (first error: %w)", failed, count, firstErr)
	}
	fmt.Printf("  Done: %d nodes created (%d already existed)\n", count-existing, existing)
	return nil
}

func DeleteKWOKNodes(clientset kubernetes.Interface) error {
	ctx := context.Background()
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "type=kwok",
	})
	if err != nil {
		return fmt.Errorf("listing KWOK nodes: %w", err)
	}

	for _, node := range nodes.Items {
		_ = clientset.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{})
	}

	return nil
}

func ReadNodesFromK8s(clientset kubernetes.Interface) ([]types.Node, error) {
	ctx := context.Background()
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "type=kwok",
	})
	if err != nil {
		return nil, fmt.Errorf("listing nodes: %w", err)
	}

	var nodes []types.Node

	for i := range nodeList.Items {
		if n, ok := NodeFromK8s(&nodeList.Items[i]); ok {
			nodes = append(nodes, n)
		}
	}

	return nodes, nil
}

func NodeFromK8s(k8sNode *corev1.Node) (types.Node, bool) {
	labels := k8sNode.Labels
	if labels == nil {
		return types.Node{}, false
	}

	cpuCores := parseUint32(labels["cpu_capacity"], 8)
	memory := parseUint32(labels["memory_gb"], 16)
	storage := parseUint32(labels["storage"], 100)
	ioSpeed := parseUint32(labels["iospeed"], 100)
	bandwidth := parseUint32(labels["bandwidth"], 100)
	ping := parseUint32(labels["latency"], 10)
	lcache := parseUint32(labels["l_cache"], 8)

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

	return types.Node{
		Name:             k8sNode.Name,
		CPUCores:         cpuCores,
		Memory:           memory,
		Storage:          storage,
		Bandwidth:        bandwidth,
		IOSpeed:          ioSpeed,
		Ping:             ping,
		LCache:           lcache,
		Exposure:         exposure,
		Isolation:        isolation,
		EncryptedStorage: encStorage,
		EncryptedTraffic: encTraffic,
		Region:           region,
		Cluster:          cluster,
	}, true
}

func RegionLabelValue(region types.Region) string {
	return regionLabelValue(string(region))
}

func WaitForNodes(clientset kubernetes.Interface, minCount int, timeout time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: "type=kwok",
		})
		if err == nil && len(nodeList.Items) >= minCount {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timed out waiting for %d KWOK nodes", minCount)
}

func parseUint32(s string, defaultVal uint32) uint32 {
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return defaultVal
	}
	return uint32(v)
}

func parseTrafficEncryption(s string) *types.TrafficEncryption {
	var dt, ispeed, pl, lat, jit float64
	n, err := fmt.Sscanf(s, "%f-%f-%f-%f-%f", &dt, &ispeed, &pl, &lat, &jit)
	if err != nil || n != 5 {
		return nil
	}
	return &types.TrafficEncryption{
		Dt:     dt,
		Ispeed: ispeed,
		Pl:     pl,
		Lat:    lat,
		Jit:    jit,
	}
}
