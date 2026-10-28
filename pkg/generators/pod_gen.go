package generators

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

type PodSizeSpec struct {
	CPU       string
	Memory    string
	CPUMilli  int
	MemoryMB  int
	StorageGB int
	Bandwidth int
	IOSpeed   int
	Latency   int
	Image     string
}

var podSizeSpecs = map[string]PodSizeSpec{
	"small":  {CPU: "4", Memory: "5Gi", CPUMilli: 4000, MemoryMB: 5120, StorageGB: 2, Bandwidth: 10, IOSpeed: 200, Latency: 100, Image: "nginx"},
	"medium": {CPU: "8", Memory: "10Gi", CPUMilli: 8000, MemoryMB: 10240, StorageGB: 4, Bandwidth: 50, IOSpeed: 500, Latency: 50, Image: "tomcat:8.0"},
	"large":  {CPU: "16", Memory: "20Gi", CPUMilli: 16000, MemoryMB: 20480, StorageGB: 8, Bandwidth: 100, IOSpeed: 1000, Latency: 200, Image: "k8s.gcr.io/pause:2.0"},
}

var podSizeNames = []string{"small", "medium", "large"}

var realPodSizeSpecs = map[string]PodSizeSpec{
	"small":  {CPU: "100m", Memory: "128Mi", CPUMilli: 100, MemoryMB: 128, StorageGB: 1, Bandwidth: 10, IOSpeed: 200, Latency: 100, Image: "registry.k8s.io/pause:3.9"},
	"medium": {CPU: "200m", Memory: "256Mi", CPUMilli: 200, MemoryMB: 256, StorageGB: 1, Bandwidth: 50, IOSpeed: 500, Latency: 50, Image: "registry.k8s.io/pause:3.9"},
	"large":  {CPU: "400m", Memory: "512Mi", CPUMilli: 400, MemoryMB: 512, StorageGB: 2, Bandwidth: 100, IOSpeed: 1000, Latency: 200, Image: "registry.k8s.io/pause:3.9"},
}

type PodGenConfig struct {
	Namespace    string
	Count        int
	ClusterCount int

	Profile string

	PrivacyRatio float64

	Seed int64

	HomeClusters   []string
	ClusterClients map[string]kubernetes.Interface
}

func (c PodGenConfig) sizeSpecs() map[string]PodSizeSpec {
	switch c.Profile {
	case "real":
		return realPodSizeSpecs
	}
	return podSizeSpecs
}

type PodInfo struct {
	Name               string `json:"name"`
	Namespace          string `json:"namespace"`
	SizeName           string `json:"size_name"`
	CPUMilli           uint32 `json:"cpu_milli"`
	MemoryMB           uint32 `json:"memory_mb"`
	StorageGB          uint32 `json:"storage_gb"`
	Bandwidth          uint32 `json:"bandwidth"`
	IOSpeed            uint32 `json:"io_speed"`
	Latency            uint32 `json:"latency"`
	GDPRRequired       bool   `json:"gdpr_required"`
	IsolationRequired  bool   `json:"isolation_required"`
	EncryptionRequired bool   `json:"encryption_required"`
	SchedulingPriority string `json:"scheduling_priority,omitempty"`

	Cluster string `json:"cluster,omitempty"`
}

func (pi *PodInfo) ToPod() types.Pod {
	return types.Pod{
		Name:               pi.Name,
		Namespace:          pi.Namespace,
		CPU:                pi.CPUMilli,
		Memory:             pi.MemoryMB,
		Storage:            pi.StorageGB,
		Bandwidth:          pi.Bandwidth,
		IOSpeed:            pi.IOSpeed,
		Latency:            pi.Latency,
		GDPRRequired:       pi.GDPRRequired,
		IsolationRequired:  pi.IsolationRequired,
		EncryptionRequired: pi.EncryptionRequired,
		SchedulingPriority: pi.SchedulingPriority,
	}
}

type podCreateWork struct {
	index int
	pod   *corev1.Pod
	info  PodInfo
}

func CreatePods(clientset kubernetes.Interface, cfg PodGenConfig) ([]PodInfo, map[string]time.Time, error) {
	return CreatePodsPaced(clientset, cfg, 0, 0, nil)
}

func CreatePodsPaced(clientset kubernetes.Interface, cfg PodGenConfig, batch int, interval time.Duration, onCreated func(PodInfo)) ([]PodInfo, map[string]time.Time, error) {
	namespace := cfg.Namespace
	count := cfg.Count
	specs := cfg.sizeSpecs()
	names := podSizeNames
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	ctx := context.Background()

	const numWorkers = 50

	homeCluster := ""
	homeLeft := 0

	if batch > 0 && interval > 0 {
		fmt.Printf("  Creating %d pods in namespace '%s': %d every %s (%.1f pods/s offered)...\n",
			count, namespace, batch, interval, float64(batch)/interval.Seconds())
	} else {
		fmt.Printf("  Creating %d pods in namespace '%s' (%d parallel workers)...\n", count, namespace, numWorkers)
	}

	podInfos := make([]PodInfo, count)
	createTimes := make(map[string]time.Time, count)
	var createMu sync.Mutex
	workCh := make(chan podCreateWork, count)
	works := make([]podCreateWork, 0, count)

	for i := 0; i < count; i++ {
		sizeName := names[rng.Intn(len(names))]
		spec := specs[sizeName]

		gdprRequired := false
		isolationRequired := false
		encryptionRequired := false
		if cfg.PrivacyRatio > 0 && rng.Float64() < cfg.PrivacyRatio {
			gdprRequired = true
			isolationRequired = rng.Float64() < 0.6
			encryptionRequired = rng.Float64() < 0.6
		}

		schedulingPriority := ""

		annotations := map[string]string{
			"placement-gates/bandwidth":           strconv.Itoa(spec.Bandwidth),
			"placement-gates/io-speed":            strconv.Itoa(spec.IOSpeed),
			"placement-gates/latency":             strconv.Itoa(spec.Latency),
			"placement-gates/gdpr-required":       strconv.FormatBool(gdprRequired),
			"placement-gates/isolation-required":  strconv.FormatBool(isolationRequired),
			"placement-gates/encryption-required": strconv.FormatBool(encryptionRequired),
		}
		if schedulingPriority != "" {
			annotations["placement-gates/scheduling-priority"] = schedulingPriority
		}

		podName := fmt.Sprintf("bench-pod-%d", i)

		if len(cfg.HomeClusters) > 0 {
			if homeLeft <= 0 {
				homeCluster = cfg.HomeClusters[rng.Intn(len(cfg.HomeClusters))]
				homeLeft = 3 + rng.Intn(8)
			}
			homeLeft--
		}
		labels := map[string]string{
			"app":      "gated-bench",
			"pod-size": sizeName,
		}
		if homeCluster != "" {
			labels["placement-gates/home-cluster"] = homeCluster
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        podName,
				Namespace:   namespace,
				Labels:      labels,
				Annotations: annotations,
			},
			Spec: corev1.PodSpec{
				SchedulerName:                 "gated-scheduler",
				TerminationGracePeriodSeconds: func() *int64 { var z int64; return &z }(),
				Tolerations: []corev1.Toleration{
					{
						Key:      "kwok.x-k8s.io/node",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				Containers: []corev1.Container{
					{
						Name:  "workload",
						Image: spec.Image,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(spec.CPU),
								corev1.ResourceMemory: resource.MustParse(spec.Memory),
							},
						},
					},
				},
			},
		}

		info := PodInfo{
			Name:               podName,
			Namespace:          namespace,
			SizeName:           sizeName,
			CPUMilli:           uint32(spec.CPUMilli),
			MemoryMB:           uint32(spec.MemoryMB),
			StorageGB:          uint32(spec.StorageGB),
			Bandwidth:          uint32(spec.Bandwidth),
			IOSpeed:            uint32(spec.IOSpeed),
			Latency:            uint32(spec.Latency),
			GDPRRequired:       gdprRequired,
			IsolationRequired:  isolationRequired,
			EncryptionRequired: encryptionRequired,
			SchedulingPriority: schedulingPriority,
			Cluster:            homeCluster,
		}

		podInfos[i] = info
		works = append(works, podCreateWork{index: i, pod: pod, info: info})
	}

	go func() {
		defer close(workCh)
		if batch <= 0 || interval <= 0 {
			for _, w := range works {
				workCh <- w
			}
			return
		}
		next := time.Now()
		for i := 0; i < len(works); i += batch {
			end := i + batch
			if end > len(works) {
				end = len(works)
			}
			for _, w := range works[i:end] {
				workCh <- w
			}
			if end < len(works) {
				next = next.Add(interval)
				time.Sleep(time.Until(next))
			}
		}
	}()

	var wg sync.WaitGroup
	var created atomic.Int64
	var createConflicts, createErrors atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr error

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workCh {
				target := clientset
				if work.info.Cluster != "" && cfg.ClusterClients != nil {
					target = cfg.ClusterClients[work.info.Cluster]
				}
				_, err := target.CoreV1().Pods(namespace).Create(ctx, work.pod, metav1.CreateOptions{})
				ct := time.Now()
				if err != nil {
					if apierrors.IsAlreadyExists(err) {
						createConflicts.Add(1)
					} else {
						createErrors.Add(1)
						firstErrMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						firstErrMu.Unlock()
					}
				}
				createMu.Lock()
				createTimes[work.info.Name] = ct
				createMu.Unlock()
				if err == nil && onCreated != nil {
					onCreated(work.info)
				}
				n := created.Add(1)
				if n%500 == 0 {
					fmt.Printf("    %d/%d pods created...\n", n, count)
				}
			}
		}()
	}

	wg.Wait()

	if n := createConflicts.Load(); n > 0 {
		return nil, nil, fmt.Errorf("%d of %d pods already existed: the namespace was not clean, "+
			"so this run would report stale pods as scheduling failures", n, count)
	}
	if n := createErrors.Load(); n > 0 {
		return nil, nil, fmt.Errorf("%d of %d pods could not be created (first error: %v)", n, count, firstErr)
	}
	fmt.Printf("  Done: %d pods created (%d workers)\n", count, numWorkers)
	return podInfos, createTimes, nil
}

func ResetPods(clientset kubernetes.Interface, namespace string) error {
	return ResetPodsWithTimeout(clientset, namespace, 30*time.Second)
}

func ResetPodsWithTimeout(clientset kubernetes.Interface, namespace string, timeout time.Duration) error {
	ctx := context.Background()

	var grace int64
	if err := clientset.CoreV1().Pods(namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{GracePeriodSeconds: &grace}, metav1.ListOptions{}); err != nil {
		return fmt.Errorf("deleting pods in %s: %w", namespace, err)
	}

	deadline := time.Now().Add(timeout)
	last := -1
	for time.Now().Before(deadline) {
		podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err == nil {
			if len(podList.Items) == 0 {
				return nil
			}
			last = len(podList.Items)
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("pod cleanup in namespace %s did not finish within %s (%d pods remain); "+
		"continuing would corrupt the run", namespace, timeout, last)
}

func BindPod(clientset kubernetes.Interface, podName, namespace, nodeName string) error {
	ctx := context.Background()

	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Target: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       nodeName,
		},
	}

	return clientset.CoreV1().Pods(namespace).Bind(ctx, binding, metav1.CreateOptions{})
}
