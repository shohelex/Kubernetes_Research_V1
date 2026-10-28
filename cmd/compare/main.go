package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/policy"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

type CompareResult struct {
	Scheduler        string  `json:"scheduler"`
	Pods             int     `json:"pods"`
	Nodes            int     `json:"nodes"`
	Scheduled        int     `json:"scheduled"`
	Failed           int     `json:"failed"`
	ThroughputPodSec float64 `json:"throughput_pods_sec"`
	TotalDurationMs  float64 `json:"total_duration_ms"`
	P50LatencyMs     float64 `json:"p50_latency_ms"`
	P95LatencyMs     float64 `json:"p95_latency_ms"`
	P99LatencyMs     float64 `json:"p99_latency_ms"`
	P999LatencyMs    float64 `json:"p999_latency_ms"`
	MeanLatencyMs    float64 `json:"mean_latency_ms"`
	CreationTimeMs   float64 `json:"creation_time_ms"`
	TimingMethod     string  `json:"timing_method"`

	ArrivalBatch           int     `json:"arrival_batch"`
	ArrivalIntervalMs      int     `json:"arrival_interval_ms"`
	OfferedRatePodSec      float64 `json:"offered_rate_pods_sec"`
	SubmissionMs           float64 `json:"submission_ms"`
	LastCreateToLastBindMs float64 `json:"last_create_to_last_bind_ms"`
	ClientQPS              float64 `json:"client_qps"`
	ClientBurst            int     `json:"client_burst"`

	PrivacyRatio    float64 `json:"privacy_ratio,omitempty"`
	PrivacyAffinity bool    `json:"privacy_affinity"`

	PrivacyPreferred bool `json:"privacy_preferred,omitempty"`

	Unschedulable int `json:"unschedulable"`

	RejectedAtAdmission  int `json:"rejected_at_admission"`
	PrivacyPods          int `json:"privacy_pods,omitempty"`
	ResidencyViolations  int `json:"residency_violations"`
	IsolationViolations  int `json:"isolation_violations"`
	EncryptionViolations int `json:"encryption_violations"`

	BoundWithAffinity     int `json:"bound_with_affinity"`
	BoundWithoutAffinity  int `json:"bound_without_affinity"`
	ViolatingWithAffinity int `json:"violating_with_affinity"`

	UnconstrainedOnCompliant    int   `json:"unconstrained_on_compliant"`
	UnconstrainedOnCompliantCPU int64 `json:"unconstrained_on_compliant_cpu_m"`

	KarmadaMembers       string         `json:"karmada_members,omitempty"`
	KarmadaUnschedulable int            `json:"karmada_unschedulable,omitempty"`
	MemberBound          map[string]int `json:"member_bound,omitempty"`

	ClaimedBy          map[string]int `json:"claimed_by,omitempty"`
	ResidencyClaimedBy map[string]int `json:"residency_claimed_by,omitempty"`
	IdleTimeoutSec     int            `json:"idle_timeout_sec,omitempty"`
	EndedIdle          bool           `json:"ended_idle,omitempty"`
}

func main() {
	home, _ := os.UserHomeDir()
	defaultKubeconfig := filepath.Join(home, ".kwok", "clusters", "gated-bench", "kubeconfig.yaml")

	kubeconfig := flag.String("kubeconfig", defaultKubeconfig, "kubeconfig")
	nodes := flag.Int("nodes", 500, "number of KWOK nodes")
	pods := flag.Int("pods", 500, "number of pods")
	schedulerName := flag.String("scheduler", "", "schedulerName for the pods (empty: kube-scheduler)")
	namespace := flag.String("namespace", "compare-bench", "namespace for the pods")
	output := flag.String("output", "", "report file")
	timeoutSec := flag.Int("timeout", 300, "seconds to wait for the pods")
	skipNodes := flag.Bool("skip-nodes", false, "reuse the nodes already in the cluster")
	seed := flag.Int64("seed", 0, "random seed (0 uses the clock)")
	clientQPS := flag.Float64("client-qps", 5000, "client rate limit")
	clientBurst := flag.Int("client-burst", 10000, "client burst")
	arrivalBatch := flag.Int("arrival-batch", 0, "pods per arrival group (0: all at once)")
	arrivalIntervalMs := flag.Int("arrival-interval-ms", 0, "ms between arrival groups")
	privacyRatio := flag.Float64("privacy-ratio", 0, "fraction of pods with privacy requirements")
	privacyAffinityOn := flag.Bool("privacy-affinity", true, "give those pods the required node affinity")
	privacyPreferred := flag.Bool("privacy-preferred", false, "use a preferred affinity instead")
	waitAll := flag.Bool("wait-all", false, "wait until every pod is bound")
	karmadaMembersFlag := flag.String("karmada-members", "", "name=kubeconfig,... of Karmada member clusters")
	idleTimeoutSec := flag.Int("idle-timeout", 0, "stop this many seconds after the last binding")

	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load kubeconfig: %v\n", err)
		os.Exit(1)
	}
	config.QPS = float32(*clientQPS)
	config.Burst = *clientBurst
	appID := fmt.Sprintf("compare-bench-%d", time.Now().UnixNano())
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	members, err := parseMembers(*karmadaMembersFlag, *clientQPS, *clientBurst)
	if err != nil {
		fmt.Fprintf(os.Stderr, "--karmada-members: %v\n", err)
		os.Exit(1)
	}
	karmada := len(members) > 0
	var bindings dynamic.Interface
	if karmada {
		if bindings, err = dynamic.NewForConfig(config); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create the Karmada client: %v\n", err)
			os.Exit(1)
		}
	}

	schedulerLabel := *schedulerName
	if schedulerLabel == "" {
		schedulerLabel = "default-scheduler (kube-scheduler)"
	}
	if karmada {
		schedulerLabel = "karmada (members' kube-scheduler)"
	}

	fmt.Println("============================================================")
	fmt.Printf("  SCHEDULER COMPARISON BENCHMARK\n")
	fmt.Printf("  Scheduler: %s\n", schedulerLabel)
	fmt.Printf("  Nodes: %d | Pods: %d\n", *nodes, *pods)
	fmt.Printf("  Timing: informer\n")
	fmt.Println("============================================================")

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: *namespace}}
	_, _ = clientset.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	for _, m := range members {
		_, _ = m.client.CoreV1().Namespaces().Create(ctx, ns.DeepCopy(), metav1.CreateOptions{})
	}

	fmt.Println("  Cleaning up existing pods...")
	_ = clientset.CoreV1().Pods(*namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{},
		metav1.ListOptions{},
	)
	waitForPodCleanup(ctx, clientset, *namespace)
	if karmada {
		waitForKarmadaCleanup(ctx, bindings, members, *namespace)
	}

	if !*skipNodes && !karmada {
		createKWOKNodes(ctx, clientset, *nodes)
	}

	if !karmada {
		fmt.Println("  Waiting for nodes...")
		waitForNodes(ctx, clientset, *nodes)
	}

	watched := []member{{name: "", client: clientset}}
	if karmada {
		watched = members
	}

	var mu sync.Mutex
	podScheduleTimes := make(map[string]time.Time)
	podCluster := make(map[string]string)

	karmadaPending := make(map[string]bool)
	var lastEvent atomic.Int64
	lastEvent.Store(time.Now().UnixNano())

	refused := make(map[string]bool)
	var scheduledCount atomic.Int64

	var target atomic.Int64
	target.Store(int64(*pods))
	allDone := make(chan struct{})
	allDoneClosed := false

	observe := func(cluster string, obj interface{}) {
		pod, ok := obj.(*corev1.Pod)
		if !ok || pod.Labels["app"] != "compare-bench" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if pod.Spec.NodeName != "" {
			if _, exists := podScheduleTimes[pod.Name]; !exists {
				podScheduleTimes[pod.Name] = time.Now()
				podCluster[pod.Name] = cluster
				delete(refused, pod.Name)
				lastEvent.Store(time.Now().UnixNano())
				n := scheduledCount.Add(1)
				if n%100 == 0 {
					fmt.Printf("    %d/%d pods scheduled...\n", n, *pods)
				}
			}
		} else if !*waitAll && unschedulable(pod) {
			if !refused[pod.Name] {
				lastEvent.Store(time.Now().UnixNano())
			}
			refused[pod.Name] = true
		}
		if len(podScheduleTimes)+len(refused) >= int(target.Load()) && !allDoneClosed {
			allDoneClosed = true
			close(allDone)
		}
	}

	for _, w := range watched {
		factory := informers.NewSharedInformerFactoryWithOptions(w.client, 0,
			informers.WithNamespace(*namespace),
		)
		cluster := w.name
		factory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { observe(cluster, obj) },
			UpdateFunc: func(_, newObj interface{}) { observe(cluster, newObj) },
		})

		factory.Start(ctx.Done())
		factory.WaitForCacheSync(ctx.Done())
	}
	fmt.Println("  Informer synced and watching...")
	if karmada {
		go watchKarmadaPending(ctx, bindings, *namespace, func(name string) {
			mu.Lock()
			defer mu.Unlock()
			if _, bound := podScheduleTimes[name]; !bound && !refused[name] && !karmadaPending[name] {
				karmadaPending[name] = true
				lastEvent.Store(time.Now().UnixNano())
			}
		})
	}

	fmt.Printf("  Creating %d pods...\n", *pods)
	e2eStart := time.Now()
	podCreateTimes, podReqs, rejectedCount := createBenchPods(ctx, clientset, *namespace, *pods, 50, *schedulerName, appID,
		*arrivalBatch, time.Duration(*arrivalIntervalMs)*time.Millisecond, *seed, *privacyRatio, *privacyAffinityOn, *privacyPreferred, karmada)
	creationDone := time.Now()
	if creationDone.UnixNano() > lastEvent.Load() {
		lastEvent.Store(creationDone.UnixNano())
	}
	creationMs := float64(creationDone.Sub(e2eStart).Nanoseconds()) / 1e6
	fmt.Printf("  Pod creation done in %.1f ms\n", creationMs)
	if rejectedCount > 0 {
		fmt.Printf("  %d pods refused at admission\n", rejectedCount)
		mu.Lock()
		target.Store(int64(*pods - rejectedCount))
		if len(podScheduleTimes)+len(refused) >= int(target.Load()) && !allDoneClosed {
			allDoneClosed = true
			close(allDone)
		}
		mu.Unlock()
	}

	fmt.Println("  Waiting for scheduling completion...")
	deadline := time.After(time.Duration(*timeoutSec) * time.Second)
	tick := time.NewTicker(time.Second)
	endedIdle := false
wait:
	for {
		select {
		case <-allDone:
			fmt.Println("  All pods scheduled!")
			break wait
		case <-tick.C:
			if *idleTimeoutSec > 0 && time.Since(time.Unix(0, lastEvent.Load())) > time.Duration(*idleTimeoutSec)*time.Second {
				endedIdle = true
				fmt.Printf("  No binding or refusal for %d s; ending the run. Scheduled: %d/%d\n", *idleTimeoutSec, scheduledCount.Load(), *pods)
				break wait
			}
		case <-deadline:
			fmt.Printf("  TIMEOUT after %d seconds. Scheduled: %d/%d\n",
				*timeoutSec, scheduledCount.Load(), *pods)

			actual := 0
			for _, w := range watched {
				podList, err := w.client.CoreV1().Pods(*namespace).List(ctx, metav1.ListOptions{
					LabelSelector: "app=compare-bench",
				})
				if err != nil {
					continue
				}
				mu.Lock()
				for _, pod := range podList.Items {
					if pod.Spec.NodeName != "" {
						actual++
						if _, exists := podScheduleTimes[pod.Name]; !exists {
							podScheduleTimes[pod.Name] = time.Now()
							podCluster[pod.Name] = w.name
						}
					}
				}
				mu.Unlock()
			}
			fmt.Printf("  Reconciliation: %d/%d actually scheduled\n", actual, *pods)
			break wait
		}
	}
	tick.Stop()

	cancel()

	var claimedBy, residencyClaimedBy map[string]int
	if karmada {
		claimedBy, residencyClaimedBy = claimingPolicies(context.Background(), bindings, *namespace, podReqs)
		fmt.Printf("  Claimed by: %v; the residency-bound pods: %v\n", claimedBy, residencyClaimedBy)
	}

	var pc placementCheck
	if *privacyRatio > 0 {
		for _, w := range watched {
			pc.add(countViolations(context.Background(), w.client, *namespace, podReqs))
		}
		fmt.Printf("  Violations: residency %d, isolation %d, encryption %d (%d pods with requirements)\n",
			pc.resid, pc.iso, pc.enc, len(podReqs))
		fmt.Printf("  Bound with a required node affinity: %d (%d of them in violation); without: %d\n",
			pc.withAffinity, pc.violatingWithAffinity, pc.withoutAffinity)
		fmt.Printf("  Pods without a residency requirement on compliant nodes: %d (%d millicores)\n",
			pc.unconstrainedOnCompliant, pc.unconstrainedOnCompliantCPU)
	}

	mu.Lock()
	scheduled := len(podScheduleTimes)
	failed := *pods - scheduled
	karmadaUnschedulable := 0
	for name := range karmadaPending {
		if _, bound := podScheduleTimes[name]; !bound && !refused[name] {
			karmadaUnschedulable++
		}
	}
	refusedCount := len(refused) + karmadaUnschedulable
	var memberBound map[string]int
	if karmada {
		memberBound = make(map[string]int, len(members))
		for _, m := range members {
			memberBound[m.name] = 0
		}
		for name := range podScheduleTimes {
			memberBound[podCluster[name]]++
		}
	}

	var lastScheduledTime time.Time
	for _, t := range podScheduleTimes {
		if t.After(lastScheduledTime) {
			lastScheduledTime = t
		}
	}

	var lastCreateTime time.Time
	for _, t := range podCreateTimes {
		if t.After(lastCreateTime) {
			lastCreateTime = t
		}
	}
	lagMs := 0.0
	if !lastCreateTime.IsZero() && lastScheduledTime.After(lastCreateTime) {
		lagMs = float64(lastScheduledTime.Sub(lastCreateTime).Nanoseconds()) / 1e6
	}
	submissionMs := 0.0
	if !lastCreateTime.IsZero() {
		submissionMs = float64(lastCreateTime.Sub(e2eStart).Nanoseconds()) / 1e6
	}
	offeredRate := 0.0
	if *arrivalBatch > 0 && *arrivalIntervalMs > 0 {
		offeredRate = float64(*arrivalBatch) * 1000 / float64(*arrivalIntervalMs)
	}

	latencies := make([]float64, 0, len(podScheduleTimes))
	for podName, schedTime := range podScheduleTimes {
		createTime, ok := podCreateTimes[podName]
		if !ok {
			createTime = e2eStart
		}
		latMs := float64(schedTime.Sub(createTime).Nanoseconds()) / 1e6
		if latMs < 0 {
			latMs = 0
		}
		latencies = append(latencies, latMs)
	}
	mu.Unlock()

	sort.Float64s(latencies)

	totalDuration := lastScheduledTime.Sub(e2eStart)
	throughput := 0.0
	if totalDuration > 0 {
		throughput = float64(scheduled) / totalDuration.Seconds()
	}

	p50, p95, p99, p999, mean := 0.0, 0.0, 0.0, 0.0, 0.0
	if len(latencies) > 0 {
		p50 = percentile(latencies, 50)
		p95 = percentile(latencies, 95)
		p99 = percentile(latencies, 99)
		p999 = percentile(latencies, 99.9)
		var sum float64
		for _, v := range latencies {
			sum += v
		}
		mean = sum / float64(len(latencies))
	}

	result := CompareResult{
		Scheduler:                   schedulerLabel,
		Pods:                        *pods,
		Nodes:                       *nodes,
		Scheduled:                   scheduled,
		Failed:                      failed,
		ThroughputPodSec:            math.Round(throughput*10) / 10,
		TotalDurationMs:             math.Round(float64(totalDuration.Nanoseconds())/1e6*100) / 100,
		P50LatencyMs:                math.Round(p50*1000) / 1000,
		P95LatencyMs:                math.Round(p95*1000) / 1000,
		P99LatencyMs:                math.Round(p99*1000) / 1000,
		P999LatencyMs:               math.Round(p999*1000) / 1000,
		MeanLatencyMs:               math.Round(mean*1000) / 1000,
		CreationTimeMs:              math.Round(creationMs*100) / 100,
		TimingMethod:                "SharedInformer (nanosecond precision, reliable event delivery)",
		ArrivalBatch:                *arrivalBatch,
		ArrivalIntervalMs:           *arrivalIntervalMs,
		OfferedRatePodSec:           math.Round(offeredRate*10) / 10,
		SubmissionMs:                math.Round(submissionMs*100) / 100,
		LastCreateToLastBindMs:      math.Round(lagMs*100) / 100,
		ClientQPS:                   *clientQPS,
		ClientBurst:                 *clientBurst,
		PrivacyRatio:                *privacyRatio,
		PrivacyAffinity:             *privacyRatio > 0 && *privacyAffinityOn,
		PrivacyPreferred:            *privacyRatio > 0 && *privacyAffinityOn && *privacyPreferred,
		Unschedulable:               refusedCount,
		RejectedAtAdmission:         rejectedCount,
		PrivacyPods:                 len(podReqs),
		ResidencyViolations:         pc.resid,
		IsolationViolations:         pc.iso,
		EncryptionViolations:        pc.enc,
		BoundWithAffinity:           pc.withAffinity,
		BoundWithoutAffinity:        pc.withoutAffinity,
		ViolatingWithAffinity:       pc.violatingWithAffinity,
		UnconstrainedOnCompliant:    pc.unconstrainedOnCompliant,
		UnconstrainedOnCompliantCPU: pc.unconstrainedOnCompliantCPU,
		KarmadaMembers:              *karmadaMembersFlag,
		KarmadaUnschedulable:        karmadaUnschedulable,
		MemberBound:                 memberBound,
		ClaimedBy:                   claimedBy,
		ResidencyClaimedBy:          residencyClaimedBy,
		IdleTimeoutSec:              *idleTimeoutSec,
		EndedIdle:                   endedIdle,
	}

	fmt.Println()
	fmt.Println("============================================================")
	fmt.Printf("  RESULTS: %s\n", schedulerLabel)
	fmt.Println("============================================================")
	fmt.Printf("  Throughput:      %.1f pods/sec\n", result.ThroughputPodSec)
	fmt.Printf("  Total Duration:  %.2f ms (%.2f sec)\n", result.TotalDurationMs, result.TotalDurationMs/1000)
	fmt.Printf("  Scheduled:       %d / %d\n", result.Scheduled, result.Pods)
	fmt.Printf("  Failed:          %d\n", result.Failed)
	fmt.Printf("  Pod Creation:    %.2f ms\n", result.CreationTimeMs)
	if result.OfferedRatePodSec > 0 {
		fmt.Printf("  Offered rate:    %.1f pods/sec (%d every %d ms)\n", result.OfferedRatePodSec, result.ArrivalBatch, result.ArrivalIntervalMs)
		fmt.Printf("  Lag after last arrival: %.2f ms\n", result.LastCreateToLastBindMs)
	}
	fmt.Printf("  P50 Latency:     %.3f ms\n", result.P50LatencyMs)
	fmt.Printf("  P95 Latency:     %.3f ms\n", result.P95LatencyMs)
	fmt.Printf("  P99 Latency:     %.3f ms\n", result.P99LatencyMs)
	fmt.Printf("  P99.9 Latency:   %.3f ms\n", result.P999LatencyMs)
	fmt.Printf("  Mean Latency:    %.3f ms\n", result.MeanLatencyMs)
	fmt.Println("============================================================")

	if *output != "" {
		data, _ := json.MarshalIndent(result, "", "  ")
		if err := os.WriteFile(*output, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write output: %v\n", err)
		} else {
			fmt.Printf("  Results written to: %s\n", *output)
		}
	}
}

func createKWOKNodes(ctx context.Context, clientset kubernetes.Interface, count int) {
	rng := rand.New(rand.NewSource(42))

	nodeSizes := []struct {
		CPU, Memory, Storage, IOPS int
	}{
		{64, 128, 1000, 10000},
		{128, 256, 2000, 20000},
		{256, 512, 4000, 40000},
	}

	regionNames := []string{"Europe", "United States", "Asia", "Other"}
	isolationNames := []string{"none", "namespace", "network_policy", "dedicated", "vm_isolated"}

	fmt.Printf("  Creating %d KWOK nodes...\n", count)

	for i := 0; i < count; i++ {
		spec := nodeSizes[rng.Intn(len(nodeSizes))]
		region := regionNames[rng.Intn(len(regionNames))]
		exposure := "public"
		if rng.Float64() < 0.6 {
			exposure = "private"
		}
		isolation := isolationNames[rng.Intn(len(isolationNames))]
		bandwidth := 100 + rng.Intn(9901)
		latency := 1 + rng.Intn(100)
		lcache := 8 + rng.Intn(57)

		labels := map[string]string{
			"kubernetes.io/hostname": fmt.Sprintf("gated-node-%d", i),
			"type":                   "kwok",
			"region":                 region,
			"bandwidth":              strconv.Itoa(bandwidth),
			"latency":                strconv.Itoa(latency),
			"iospeed":                strconv.Itoa(spec.IOPS),
			"cpu_capacity":           strconv.Itoa(spec.CPU),
			"memory_gb":              strconv.Itoa(spec.Memory),
			"storage":                strconv.Itoa(spec.Storage),
			"l_cache":                strconv.Itoa(lcache),
			"exposure":               exposure,
			"isolation":              isolation,
		}

		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   fmt.Sprintf("gated-node-%d", i),
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

		_, _ = clientset.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})

		if (i+1)%100 == 0 {
			fmt.Printf("    %d/%d nodes created...\n", i+1, count)
		}
	}
	fmt.Printf("  Done: %d nodes created\n", count)
}

func createBenchPods(ctx context.Context, clientset kubernetes.Interface, namespace string, count, workers int, schedulerName, appID string, batch int, interval time.Duration, seed int64, privacyRatio float64, withAffinity, preferred, labelResidency bool) (map[string]time.Time, map[string]types.Pod, int) {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	reqs := make(map[string]types.Pod)

	type podWork struct {
		name string
		pod  *corev1.Pod
	}

	podSizes := []struct {
		CPU, Memory, Image string
	}{
		{"4", "5Gi", "nginx"},
		{"8", "10Gi", "tomcat:8.0"},
		{"16", "20Gi", "k8s.gcr.io/pause:2.0"},
	}

	workCh := make(chan podWork, count)
	works := make([]podWork, 0, count)
	createTimes := make(map[string]time.Time, count)
	var mu sync.Mutex

	for i := 0; i < count; i++ {
		spec := podSizes[rng.Intn(len(podSizes))]
		podName := fmt.Sprintf("compare-pod-%d", i)

		var req types.Pod
		if privacyRatio > 0 && rng.Float64() < privacyRatio {
			req.GDPRRequired = true
			req.IsolationRequired = rng.Float64() < 0.6
			req.EncryptionRequired = rng.Float64() < 0.6
			reqs[podName] = req
		}

		podSpec := corev1.PodSpec{
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
		}

		if schedulerName != "" {
			podSpec.SchedulerName = schedulerName
		}
		if withAffinity {
			podSpec.Affinity = privacyAffinity(req, preferred)
		}

		labels := map[string]string{
			"app": "compare-bench",
		}
		if labelResidency && req.GDPRRequired {
			labels["residency"] = "required"
		}

		annotations := map[string]string{}
		for k, v := range policy.Annotations(req) {
			annotations[k] = v
		}

		if schedulerName == "yunikorn" {
			labels["applicationId"] = appID
			labels["queue"] = "root.default"
			annotations["yunikorn.apache.org/app-id"] = appID
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        podName,
				Namespace:   namespace,
				Labels:      labels,
				Annotations: annotations,
			},
			Spec: podSpec,
		}

		works = append(works, podWork{name: podName, pod: pod})
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
	var created, rejected atomic.Int64

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workCh {
				_, err := clientset.CoreV1().Pods(namespace).Create(ctx, work.pod, metav1.CreateOptions{})
				createTime := time.Now()
				if err != nil {
					rejected.Add(1)
					created.Add(1)
					continue
				}
				mu.Lock()
				createTimes[work.name] = createTime
				mu.Unlock()

				n := created.Add(1)
				if n%100 == 0 {
					fmt.Printf("    %d/%d pods created...\n", n, count)
				}
			}
		}()
	}

	wg.Wait()
	return createTimes, reqs, int(rejected.Load())
}

func privacyAffinity(req types.Pod, preferred bool) *corev1.Affinity {
	return policy.PrivacyAffinity(req, preferred)
}

func unschedulable(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
			return true
		}
	}
	return false
}

type placementCheck struct {
	resid, iso, enc       int
	withAffinity          int
	withoutAffinity       int
	violatingWithAffinity int

	unconstrainedOnCompliant    int
	unconstrainedOnCompliantCPU int64
}

func (pc *placementCheck) add(o placementCheck) {
	pc.resid += o.resid
	pc.iso += o.iso
	pc.enc += o.enc
	pc.withAffinity += o.withAffinity
	pc.withoutAffinity += o.withoutAffinity
	pc.violatingWithAffinity += o.violatingWithAffinity
	pc.unconstrainedOnCompliant += o.unconstrainedOnCompliant
	pc.unconstrainedOnCompliantCPU += o.unconstrainedOnCompliantCPU
}

type member struct {
	name   string
	client kubernetes.Interface
}

func parseMembers(spec string, qps float64, burst int) ([]member, error) {
	if spec == "" {
		return nil, nil
	}
	var out []member
	for _, kv := range strings.Split(spec, ",") {
		name, path, ok := strings.Cut(kv, "=")
		if !ok || name == "" || path == "" {
			return nil, fmt.Errorf("expected name=kubeconfig, got %q", kv)
		}
		rc, err := clientcmd.BuildConfigFromFlags("", path)
		if err != nil {
			return nil, fmt.Errorf("member %s: %w", name, err)
		}
		rc.QPS = float32(qps)
		rc.Burst = burst
		c, err := kubernetes.NewForConfig(rc)
		if err != nil {
			return nil, fmt.Errorf("member %s: %w", name, err)
		}
		out = append(out, member{name: name, client: c})
	}
	return out, nil
}

var resourceBindings = schema.GroupVersionResource{Group: "work.karmada.io", Version: "v1alpha2", Resource: "resourcebindings"}

func watchKarmadaPending(ctx context.Context, dyn dynamic.Interface, namespace string, pending func(name string)) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		list, err := dyn.Resource(resourceBindings).Namespace(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}
		for _, rb := range list.Items {
			name, _, _ := unstructured.NestedString(rb.Object, "spec", "resource", "name")
			conds, _, _ := unstructured.NestedSlice(rb.Object, "status", "conditions")
			for _, c := range conds {
				m, ok := c.(map[string]interface{})
				if ok && m["type"] == "Scheduled" && m["status"] == "False" && name != "" {
					pending(name)
				}
			}
		}
	}
}

func claimingPolicies(ctx context.Context, dyn dynamic.Interface, namespace string, reqs map[string]types.Pod) (all, residency map[string]int) {
	all, residency = map[string]int{}, map[string]int{}
	list, err := dyn.Resource(resourceBindings).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		fmt.Printf("  WARNING: listing ResourceBindings: %v\n", err)
		return
	}
	for _, rb := range list.Items {
		policy := claimingPolicy(rb.GetAnnotations())
		all[policy]++
		name, _, _ := unstructured.NestedString(rb.Object, "spec", "resource", "name")
		if reqs[name].GDPRRequired {
			residency[policy]++
		}
	}
	return
}

func claimingPolicy(a map[string]string) string {
	if n := a["propagationpolicy.karmada.io/name"]; n != "" {
		return "PropagationPolicy " + a["propagationpolicy.karmada.io/namespace"] + "/" + n
	}
	if n := a["clusterpropagationpolicy.karmada.io/name"]; n != "" {
		return "ClusterPropagationPolicy " + n
	}
	return "none"
}

func waitForKarmadaCleanup(ctx context.Context, dyn dynamic.Interface, members []member, namespace string) {
	for i := 0; i < 300; i++ {
		left := 0
		if list, err := dyn.Resource(resourceBindings).Namespace(namespace).List(ctx, metav1.ListOptions{}); err == nil {
			left += len(list.Items)
		}
		for _, m := range members {
			if list, err := m.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{}); err == nil {
				left += len(list.Items)
			}
		}
		if left == 0 {
			return
		}
		time.Sleep(time.Second)
	}
	fmt.Println("  WARNING: bindings or member pods left after five minutes")
}

func countViolations(ctx context.Context, clientset kubernetes.Interface, namespace string, reqs map[string]types.Pod) (pc placementCheck) {
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "type=kwok"})
	if err != nil {
		fmt.Printf("  WARNING: listing nodes for the violation check: %v\n", err)
		return
	}
	nodes := make(map[string]types.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		if n, ok := generators.NodeFromK8s(&nodeList.Items[i]); ok {
			nodes[n.Name] = n
		}
	}
	podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "app=compare-bench"})
	if err != nil {
		fmt.Printf("  WARNING: listing pods for the violation check: %v\n", err)
		return
	}
	for _, p := range podList.Items {
		if p.Spec.NodeName == "" {
			continue
		}
		req, ok := reqs[p.Name]
		if !req.GDPRRequired && scheduler.GDPRCompliantRegions[nodes[p.Spec.NodeName].Region] {
			pc.unconstrainedOnCompliant++
			for _, c := range p.Spec.Containers {
				pc.unconstrainedOnCompliantCPU += c.Resources.Requests.Cpu().MilliValue()
			}
		}
		if !ok {
			continue
		}
		hasAffinity := p.Spec.Affinity != nil && p.Spec.Affinity.NodeAffinity != nil &&
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil
		if hasAffinity {
			pc.withAffinity++
		} else {
			pc.withoutAffinity++
		}
		vs := scheduler.CheckViolations(req, nodes[p.Spec.NodeName])
		if hasAffinity && len(vs) > 0 {
			pc.violatingWithAffinity++
		}
		for _, v := range vs {
			switch v {
			case scheduler.ViolationResidency:
				pc.resid++
			case scheduler.ViolationIsolation:
				pc.iso++
			case scheduler.ViolationEncryption:
				pc.enc++
			}
		}
	}
	return
}

func waitForPodCleanup(ctx context.Context, clientset kubernetes.Interface, namespace string) {
	for i := 0; i < 60; i++ {
		podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err == nil && len(podList.Items) == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForNodes(ctx context.Context, clientset kubernetes.Interface, minCount int) {
	for i := 0; i < 60; i++ {
		nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: "type=kwok",
		})
		if err == nil && len(nodeList.Items) >= minCount {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := pct / 100.0 * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}
