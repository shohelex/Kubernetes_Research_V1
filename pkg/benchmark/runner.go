package benchmark

import (
	"context"
	"errors"
	"fmt"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/auditlog"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Config struct {
	Pods             int
	Nodes            int
	Clusters         int
	Runs             int
	Priority         string
	OutputPath       string
	Kubeconfig       string
	SkipNodeCreation bool

	ClusterKubeconfigs map[string]string
	ClusterOrder       []string
	ClusterRegions     map[string]string

	Namespace string

	ClientQPS   float32
	ClientBurst int

	ArrivalBatch      int
	ArrivalIntervalMs int
	Schedulers        int
	E2ETiming         bool
	InformerTiming    bool

	RealNodes bool
	Seed      int64

	DisableReuse bool

	PodProfile   string
	PrivacyRatio float64

	RegionMode string

	ShadowRate float64

	DisablePrivacyFilter bool

	MovePenalty float64

	RecheckGates bool

	Scorer scheduler.Scorer

	ReuseClasses bool

	KeepCompliant bool

	LabelChurnMs int

	LabelChurnTarget string

	AuditLog  string
	AuditMode string
	audit     *auditlog.Writer
}

func Run(cfg Config) (Report, error) {
	fmt.Println("============================================================")
	fmt.Println("  Gated Scheduler Benchmark (Go - KWOK / Real K8s API)")
	fmt.Println("============================================================")
	fmt.Printf("  Pods: %d, Nodes: %d, Clusters: %d\n", cfg.Pods, cfg.Nodes, cfg.Clusters)
	fmt.Printf("  Runs: %d, Priority: %s\n", cfg.Runs, cfg.Priority)
	fmt.Printf("  Kubeconfig: %s\n", cfg.Kubeconfig)
	fmt.Printf("  Schedulers: %d\n", cfg.Schedulers)
	fmt.Printf("  Skip Node Creation: %v\n", cfg.SkipNodeCreation)
	if cfg.paced() {
		fmt.Printf("  Arrivals: %d pods every %d ms (%.1f pods/s offered)\n",
			cfg.ArrivalBatch, cfg.ArrivalIntervalMs, cfg.OfferedRate())
	} else {
		fmt.Printf("  Arrivals: every pod at once (burst)\n")
	}
	if cfg.InformerTiming {
		fmt.Printf("  Timing: informer\n")
	} else if cfg.E2ETiming {
		fmt.Printf("  Timing: end to end\n")
	} else {
		fmt.Printf("  Timing: scheduling only\n")
	}
	fmt.Println("============================================================")

	if cfg.ClientQPS <= 0 {
		cfg.ClientQPS = 5000
	}
	if cfg.ClientBurst <= 0 {
		cfg.ClientBurst = 10000
	}
	fmt.Printf("  API client limit: %.0f/s, burst %d\n",
		cfg.ClientQPS, cfg.ClientBurst)

	var clientset, setupClient kubernetes.Interface
	cs, err := newClusterSet(cfg)
	if err != nil {
		return Report{}, err
	}
	if cs.federated() {
		if cfg.RealNodes {
			return Report{}, fmt.Errorf("a federation of real-node clusters is not supported")
		}
		clientset, setupClient = cs.primary()
		fmt.Printf("\n  Federation: %d clusters %v, every API server answered\n", len(cs.names), cs.names)
	} else {
		restConfig, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
		if err != nil {
			return Report{}, fmt.Errorf("building kubeconfig: %w", err)
		}
		restConfig.QPS = cfg.ClientQPS
		restConfig.Burst = cfg.ClientBurst
		clientset, err = kubernetes.NewForConfig(restConfig)
		if err != nil {
			return Report{}, fmt.Errorf("creating kubernetes client: %w", err)
		}

		setupClient, err = kubernetes.NewForConfig(rest.CopyConfig(restConfig))
		if err != nil {
			return Report{}, fmt.Errorf("creating setup client: %w", err)
		}

		_, err = clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return Report{}, fmt.Errorf("cannot connect to cluster (is KWOK running?): %w", err)
		}
		fmt.Println("\n  Cluster connected OK")
	}

	if cs.federated() {
		err = cs.ensureNamespaces(cfg.Namespace)
	} else {
		err = ensureNamespace(clientset, cfg.Namespace)
	}
	if err != nil {
		return Report{}, fmt.Errorf("ensuring namespace: %w", err)
	}

	ahpWeights, cr := scheduler.GetPerformanceWeights("general", 0.10)
	fmt.Printf("\nAHP Weights (CR=%.4f): cpu=%.3f mem=%.3f stor=%.3f io=%.3f cache=%.3f\n",
		cr, ahpWeights.CPU, ahpWeights.Memory, ahpWeights.Storage, ahpWeights.IO, ahpWeights.Cache)

	pw := scheduler.SchedulingPriorityWeights[cfg.Priority]
	fmt.Printf("\n  Priority weights (%s): perf=%.1f exp=%.1f iso=%.1f enc=%.1f dpa=%.1f\n",
		cfg.Priority, pw.Performance, pw.Exposure, pw.Isolation, pw.Encryption, pw.DPA)

	scheduler.PrivacyFiltersEnabled = !cfg.DisablePrivacyFilter
	if cfg.DisablePrivacyFilter {
		fmt.Println()
		fmt.Println("  WARNING: hard privacy gates DISABLED (ablation run)")
	}

	tracker := scheduler.NewNodeResourceTracker()

	var nodes []types.Node
	if cfg.RealNodes {
		if cfg.SkipNodeCreation {
			fmt.Println("\n  Reusing existing real-node labels...")
		} else {
			fmt.Println("\n  Labelling real nodes...")
			rnCfg := generators.DefaultRealNodeConfig(cfg.Seed, cfg.Clusters)
			if cfg.RegionMode != "" {
				rnCfg.RegionMode = cfg.RegionMode
			}
			n, err := generators.LabelRealNodes(clientset, rnCfg)
			if err != nil {
				return Report{}, fmt.Errorf("labelling real nodes: %w", err)
			}
			fmt.Printf("  Labelled %d schedulable real nodes\n", n)
		}

		nodes, err = generators.ReadRealNodes(clientset)
		if err != nil {
			return Report{}, fmt.Errorf("reading real nodes: %w", err)
		}
		cores, memGB, podSlots, _ := generators.NodeAllocatableSummary(clientset)
		fmt.Printf("  Loaded %d real nodes: %d allocatable cores, %d GiB, %d pod slots\n",
			len(nodes), cores, memGB, podSlots)
		if len(nodes) == 0 {
			return Report{}, fmt.Errorf("no schedulable real nodes found")
		}
		if cfg.Pods > podSlots {
			fmt.Printf("  WARNING: %d pods but only %d pod slots\n", cfg.Pods, podSlots)
		}
	} else if cs.federated() {
		fmt.Printf("\n  Nodes: %d KWOK nodes per cluster\n", cfg.Nodes)
		nodes, err = cs.loadNodes(cfg)
		if err != nil {
			return Report{}, err
		}
		fmt.Printf("  Loaded %d nodes across %d clusters\n", len(nodes), len(cs.names))
	} else {
		if cfg.SkipNodeCreation {
			fmt.Println("\n  Reusing existing KWOK nodes...")
		} else {
			fmt.Println("\n  Cleaning existing KWOK nodes...")
			if err := generators.DeleteKWOKNodes(clientset); err != nil {
				fmt.Printf("  Warning: could not clean old nodes: %v\n", err)
			}
			time.Sleep(1 * time.Second)

			if err := generators.CreateKWOKNodes(clientset, cfg.Nodes, cfg.Clusters); err != nil {
				return Report{}, fmt.Errorf("creating KWOK nodes: %w", err)
			}
		}

		nodes, err = generators.ReadNodesFromK8s(clientset)
		if err != nil {
			return Report{}, fmt.Errorf("reading nodes from K8s: %w", err)
		}
		fmt.Printf("  Loaded %d nodes from K8s API\n", len(nodes))

		if len(nodes) == 0 {
			return Report{}, fmt.Errorf("no KWOK nodes found; ensure cluster is running and nodes were created")
		}
	}

	scheduler.Live = nil
	if cfg.LabelChurnMs > 0 {
		if cfg.RealNodes || cs.federated() {
			return Report{}, fmt.Errorf("--label-churn-ms needs KWOK nodes on one API server")
		}
		scheduler.Live = scheduler.NewLiveLabels(nodes)
		labelCtx, labelCancel := context.WithCancel(context.Background())
		defer labelCancel()
		watchNodeLabels(labelCtx, clientset, scheduler.Live)
		fmt.Printf("  Label churn: one European node moved out of Europe every %d ms (target %s); bind-time gate re-check %v\n",
			cfg.LabelChurnMs, cfg.LabelChurnTarget, cfg.RecheckGates)
	}

	var informerMu sync.Mutex
	var podScheduleTimes map[string]time.Time
	var scheduledCount atomic.Int64

	var informerTarget atomic.Int64
	var allDone chan struct{}
	var allDoneClosed bool
	var informerCancel context.CancelFunc

	if cfg.InformerTiming {
		informerCtx, cancel := context.WithCancel(context.Background())
		informerCancel = cancel

		podScheduleTimes = make(map[string]time.Time)
		allDone = make(chan struct{})
		informerTarget.Store(int64(cfg.Pods))

		observe := func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Labels["app"] != "gated-bench" || pod.Spec.NodeName == "" {
				return
			}
			informerMu.Lock()
			if _, exists := podScheduleTimes[pod.Name]; !exists {
				podScheduleTimes[pod.Name] = time.Now()
				n := scheduledCount.Add(1)
				if n%500 == 0 {
					fmt.Printf("    [informer] %d/%d pods scheduled...\n", n, cfg.Pods)
				}
				if n >= informerTarget.Load() && !allDoneClosed {
					allDoneClosed = true
					close(allDone)
				}
			}
			informerMu.Unlock()
		}
		watched := []kubernetes.Interface{clientset}
		if cs.federated() {
			watched = watched[:0]
			for _, name := range cs.names {
				watched = append(watched, cs.clients[name])
			}
		}
		for _, wc := range watched {
			factory := informers.NewSharedInformerFactoryWithOptions(wc, 0,
				informers.WithNamespace(cfg.Namespace),
			)
			factory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    observe,
				UpdateFunc: func(_, newObj interface{}) { observe(newObj) },
			})
			factory.Start(informerCtx.Done())
			factory.WaitForCacheSync(informerCtx.Done())
		}
		fmt.Printf("  SharedInformer synced on %d cluster(s), watching for pod scheduling events...\n", len(watched))
	}

	allRunMetrics := make([]RunMetrics, cfg.Runs)

	for run := 0; run < cfg.Runs; run++ {
		fmt.Printf("\n--- Run %d/%d ---\n", run+1, cfg.Runs)

		fmt.Printf("  Cleaning up pods...")

		cleanupBudget := 60*time.Second + time.Duration(cfg.Pods/20)*time.Second
		var cleanErr error
		if cs.federated() {
			cleanErr = cs.resetPods(cfg.Namespace, cleanupBudget)
		} else {
			cleanErr = generators.ResetPodsWithTimeout(clientset, cfg.Namespace, cleanupBudget)
		}
		if cleanErr != nil {
			return Report{}, fmt.Errorf("pod cleanup before run %d: %w", run+1, cleanErr)
		}
		fmt.Println(" done")

		fmt.Printf("  Resetting node resource tracking...")
		nodeNames := make([]string, len(nodes))
		for i := range nodes {
			nodeNames[i] = nodes[i].Name
		}
		tracker.Reset(nodeNames...)
		fmt.Println(" done")

		if cfg.RealNodes {
			if n, err := bookExistingPods(clientset, tracker, nodes, cfg.Namespace); err != nil {
				fmt.Printf("  Warning: could not read existing pods: %v\n", err)
			} else {
				fmt.Printf("  Booked %d existing pod(s) on the real nodes\n", n)
			}
		}

		scheduler.ResetFilterStats()

		runCfg := cfg
		if cfg.AuditLog != "" {
			w, aerr := auditlog.Open(auditPath(cfg.AuditLog, run), auditlog.Mode(cfg.AuditMode), 1000)
			if aerr != nil {
				return Report{}, fmt.Errorf("decision log for run %d: %w", run+1, aerr)
			}
			runCfg.audit = w
		}

		var runDone chan struct{}
		if cfg.InformerTiming {
			informerMu.Lock()
			podScheduleTimes = make(map[string]time.Time)
			scheduledCount.Store(0)
			informerTarget.Store(int64(cfg.Pods))
			allDone = make(chan struct{})
			allDoneClosed = false
			runDone = allDone
			informerMu.Unlock()
		}

		lowerInformerTarget := func(rm RunMetrics) {
			if !cfg.InformerTiming || rm.Failed == 0 {
				return
			}
			informerMu.Lock()
			informerTarget.Store(int64(rm.Scheduled))
			if scheduledCount.Load() >= int64(rm.Scheduled) && !allDoneClosed {
				allDoneClosed = true
				close(allDone)
			}
			informerMu.Unlock()
		}

		var e2eStartTime time.Time
		if cfg.E2ETiming || cfg.InformerTiming || cfg.paced() {
			e2eStartTime = time.Now()
		}

		podProfile := cfg.PodProfile
		if podProfile == "" {
			if cfg.RealNodes {
				podProfile = "real"
			} else {
				podProfile = "synthetic"
			}
		}

		runSeed := int64(0)
		if cfg.Seed != 0 {
			runSeed = cfg.Seed + int64(run)
		}
		genCfg := generators.PodGenConfig{
			Namespace:    cfg.Namespace,
			Count:        cfg.Pods,
			ClusterCount: cfg.Clusters,
			Profile:      podProfile,
			PrivacyRatio: cfg.PrivacyRatio,
			Seed:         runSeed,

			HomeClusters:   cs.clusterNames(),
			ClusterClients: cs.setupClients(),
		}

		var podInfos []generators.PodInfo
		var podCreateTimes map[string]time.Time
		var rm RunMetrics
		if cfg.paced() {
			pq, qerr := NewPodQueue(cfg, cfg.Pods)
			if qerr != nil {
				return Report{}, fmt.Errorf("queue backend: %w", qerr)
			}
			podCh := feedFromQueue(pq, cfg.Pods, cfg.Schedulers)
			fmt.Printf("  Scheduling with %d goroutines while pods arrive (%d every %d ms, %.1f pods/s offered)...\n",
				cfg.Schedulers, cfg.ArrivalBatch, cfg.ArrivalIntervalMs, cfg.OfferedRate())
			rmCh := make(chan RunMetrics, 1)
			go func() {
				rmCh <- executeRun(run, clientset, cs, tracker, nodes, ahpWeights, runCfg, podCh)
			}()
			var pushMu sync.Mutex
			var pushErr error
			podInfos, podCreateTimes, err = generators.CreatePodsPaced(setupClient, genCfg,
				cfg.ArrivalBatch, time.Duration(cfg.ArrivalIntervalMs)*time.Millisecond,
				func(info generators.PodInfo) {
					if perr := pq.Push(info); perr != nil {
						pushMu.Lock()
						if pushErr == nil {
							pushErr = perr
						}
						pushMu.Unlock()
					}
				})
			if err == nil {
				pushMu.Lock()
				err = pushErr
				pushMu.Unlock()
			}
			if err != nil {
				pq.Close()
				<-rmCh
				return Report{}, fmt.Errorf("creating pods for run %d: %w", run+1, err)
			}
			rm = <-rmCh
			lowerInformerTarget(rm)

			rm.StartTime = e2eStartTime
		} else {
			podInfos, podCreateTimes, err = generators.CreatePods(setupClient, genCfg)
			if err != nil {
				return Report{}, fmt.Errorf("creating pods for run %d: %w", run+1, err)
			}

			pq, qerr := NewPodQueue(cfg, len(podInfos))
			if qerr != nil {
				return Report{}, fmt.Errorf("queue backend: %w", qerr)
			}
			fmt.Printf("  Pushing %d pods to %s...\n", len(podInfos), pq.Name())
			for i := range podInfos {
				if err := pq.Push(podInfos[i]); err != nil {
					pq.Close()
					return Report{}, fmt.Errorf("pushing pod %d: %w", i, err)
				}
			}
			podCh := feedFromQueue(pq, len(podInfos), cfg.Schedulers)

			fmt.Printf("  Scheduling pods with %d goroutines...\n", cfg.Schedulers)
			rm = executeRun(run, clientset, cs, tracker, nodes, ahpWeights, runCfg, podCh)
			lowerInformerTarget(rm)

			if cfg.E2ETiming {
				rm.StartTime = e2eStartTime
			}
		}

		if runCfg.audit != nil {
			if err := runCfg.audit.Close(); err != nil {
				fmt.Printf("  WARNING: decision log: %v\n", err)
				rm.AuditErrors++
			}
			rm.AuditEntries, rm.AuditBytes = runCfg.audit.Stats()
			rm.AuditMode, rm.AuditFile = cfg.AuditMode, auditPath(cfg.AuditLog, run)
			fmt.Printf("  Decision log (%s): %d records, %d bytes -> %s\n", rm.AuditMode, rm.AuditEntries, rm.AuditBytes, rm.AuditFile)
		}

		var migrationSummary *MigrationSummary
		if rm.MigrationCount > 0 || cs.federated() {
			migrationSummary = buildMigrationSummary(rm.PodLatencies, rm.MigrationCount, rm.Scheduled)
		}

		if cfg.InformerTiming {
			select {
			case <-runDone:
				fmt.Println("    [informer] All pods confirmed scheduled!")
			case <-time.After(300 * time.Second):
				fmt.Printf("    [informer] TIMEOUT after 300s. Scheduled: %d/%d\n",
					scheduledCount.Load(), cfg.Pods)
			}

			informerMu.Lock()
			scheduled := len(podScheduleTimes)

			var lastScheduledTime time.Time
			for _, t := range podScheduleTimes {
				if t.After(lastScheduledTime) {
					lastScheduledTime = t
				}
			}

			informerLatencies := make([]float64, 0, scheduled)
			for podName, schedTime := range podScheduleTimes {
				createTime, ok := podCreateTimes[podName]
				if !ok {
					createTime = e2eStartTime
				}
				latMs := float64(schedTime.Sub(createTime).Nanoseconds()) / 1e6
				if latMs < 0 {
					latMs = 0
				}
				informerLatencies = append(informerLatencies, latMs)
			}
			informerMu.Unlock()

			sort.Float64s(informerLatencies)

			rm.StartTime = e2eStartTime
			rm.EndTime = lastScheduledTime
			rm.Scheduled = scheduled
			rm.Failed = cfg.Pods - scheduled

			rm.PodLatencies = make([]PodLatency, len(informerLatencies))
			for i, latMs := range informerLatencies {
				rm.PodLatencies[i] = PodLatency{
					TotalNs: int64(latMs * 1e6),
					Bound:   true,
				}
			}
		}

		for _, t := range podCreateTimes {
			if t.After(rm.SubmissionEnd) {
				rm.SubmissionEnd = t
			}
		}

		rm.MigrationSummary = migrationSummary
		allRunMetrics[run] = rm

		fmt.Printf("  Throughput: %.1f pods/sec | Scheduled: %d | Failed: %d\n",
			rm.ThroughputPodsPerSec(), rm.Scheduled, rm.Failed)
		fmt.Printf("  Queue Reuse: %d (%.1f%%)\n",
			rm.QueueReuseCount, rm.QueueReuseRate()*100)
		if cfg.paced() {
			fmt.Printf("  Offered: %.1f pods/s | Lag after last arrival: %.1f ms\n",
				cfg.OfferedRate(), rm.LagMs())
		}
		if rm.MigrationCount > 0 {
			fmt.Printf("  Migrations: %d (%.1f%% of scheduled pods)\n",
				rm.MigrationCount, rm.MigrationRate()*100)
		}
		scheduler.PrintFilterStats()
	}

	if cfg.InformerTiming && informerCancel != nil {
		informerCancel()
	}

	timingMode := "scheduling_only"
	if cfg.InformerTiming {
		timingMode = "informer (SharedInformer)"
	} else if cfg.E2ETiming {
		timingMode = "end_to_end"
	}
	reportCfg := ReportConfig{
		Pods:               cfg.Pods,
		Nodes:              cfg.Nodes,
		Clusters:           cfg.Clusters,
		Runs:               cfg.Runs,
		Priority:           cfg.Priority,
		TimingMode:         timingMode,
		Schedulers:         cfg.Schedulers,
		Scorer:             string(scorerOrDefault(cfg.Scorer)),
		ReuseClasses:       cfg.ReuseClasses,
		KeepCompliant:      cfg.KeepCompliant,
		RecheckGates:       cfg.RecheckGates,
		PrivacyGates:       !cfg.DisablePrivacyFilter,
		DisableReuse:       cfg.DisableReuse,
		ClientQPS:          float64(cfg.ClientQPS),
		ClientBurst:        cfg.ClientBurst,
		ArrivalBatch:       cfg.ArrivalBatch,
		ArrivalIntervalMs:  cfg.ArrivalIntervalMs,
		OfferedRatePodsSec: cfg.OfferedRate(),
		Federated:          cs.federated(),
		ClusterNames:       cs.clusterNames(),
		ClusterRegions:     cfg.ClusterRegions,
	}
	if cs.federated() || cfg.Clusters > 1 {
		reportCfg.MovePenalty = cfg.MovePenalty
	}
	if cs.federated() {
		reportCfg.Nodes = len(nodes)
		reportCfg.Clusters = len(cs.names)
	}

	report := BuildReport(reportCfg, allRunMetrics)

	fmt.Println("\n============================================================")
	fmt.Println("  AGGREGATE RESULTS")
	fmt.Println("============================================================")
	fmt.Printf("  Throughput:  %.1f +/- %.1f pods/sec (95%% CI: [%.1f, %.1f])\n",
		report.Aggregate.ThroughputPodsPerSec.Mean,
		report.Aggregate.ThroughputPodsPerSec.Std,
		report.Aggregate.ThroughputPodsPerSec.CI95Lo,
		report.Aggregate.ThroughputPodsPerSec.CI95Hi)
	fmt.Printf("  P50 Latency: %.3f ms\n", report.Aggregate.P50LatencyMs.Mean)
	fmt.Printf("  P95 Latency: %.3f ms\n", report.Aggregate.P95LatencyMs.Mean)
	fmt.Printf("  P99 Latency: %.3f ms\n", report.Aggregate.P99LatencyMs.Mean)
	fmt.Printf("  P99.9 Latency: %.3f ms\n", report.Aggregate.P999LatencyMs.Mean)
	fmt.Printf("  Queue Reuse Rate: %.2f%%\n", report.Aggregate.QueueReuseRate.Mean*100)
	fmt.Printf("  Filter Pass Rate: %.2f%%\n", report.Aggregate.FilterPassRate.Mean*100)
	fmt.Printf("  Scheduled: %.0f, Failed: %.0f\n",
		report.Aggregate.ScheduledCount.Mean, report.Aggregate.FailedCount.Mean)
	fmt.Println()
	if report.Aggregate.MigrationCount.Mean > 0 {
		fmt.Printf("  Migration Rate: %.2f%% (%d migrations)\n",
			report.Aggregate.MigrationRate.Mean*100,
			int(report.Aggregate.MigrationCount.Mean))
	}
	if len(allRunMetrics) > 0 && allRunMetrics[0].Placement != nil {
		pq := allRunMetrics[0].Placement
		fmt.Println("  Placement Quality (run 1):")
		fmt.Printf("    CPU util:  mean %.1f%% max %.1f%%\n", pq.MeanCPUUtil*100, pq.MaxCPUUtil*100)
		fmt.Printf("    Balance:   Jain %.4f (imbalance %.4f) CV %.4f\n", pq.CPUJain, pq.CPUImbal, pq.CPUCV)
		fmt.Printf("    Pods/node: Jain %.4f CV %.4f\n", pq.PodJain, pq.PodCV)
		fmt.Printf("    Nodes:     %d used, %d empty, %d over 90%% CPU (of %d)\n",
			pq.NodesUsed, pq.NodesEmpty, pq.NodesOver90, pq.NodesTotal)
		if pq.ShadowChecked > 0 {
			fmt.Printf("    Misplaced: %d/%d reused pods (%.2f%%), score regret mean %.4f (%.2f%% of best) max %.4f\n",
				pq.Misplaced, pq.ShadowChecked, pq.MisplacementRate*100,
				pq.ScoreRegret, pq.ScoreRegretRel*100, pq.ScoreRegretMax)
		}
		if allRunMetrics[0].BindConflicts > 0 || allRunMetrics[0].BindOtherErrors > 0 {
			fmt.Printf("    Bind errs: %d conflict (409), %d other\n",
				allRunMetrics[0].BindConflicts, allRunMetrics[0].BindOtherErrors)
		}
		fmt.Println()
	}
	if len(allRunMetrics) > 0 && allRunMetrics[0].Violations != nil {
		v := allRunMetrics[0].Violations
		fmt.Println("  Compliance (run 1):")
		fmt.Printf("    pods with a requirement: %d (gdpr %d, iso %d, enc %d)\n",
			v.PodsChecked, v.GDPRPods, v.IsolationPods, v.EncryptionPods)
		fmt.Printf("    VIOLATIONS: residency %d, isolation %d, encryption %d  (%d pods, %.2f%%)\n",
			v.Residency, v.Isolation, v.Encryption, v.PodsViolating, v.OverallViolationRate*100)
		if v.GDPRPods > 0 {
			fmt.Printf("    residency violation rate: %.2f%% of GDPR-bound pods\n", v.ResidencyViolationRate*100)
		}
		for _, e := range v.Examples {
			fmt.Printf("      e.g. %s\n", e)
		}
		fmt.Println()
	}
	fmt.Println("  Encryption Overhead:")
	fmt.Printf("    Enc mean lat:     %.3f ms\n", report.Aggregate.EncMeanLatMs.Mean)
	fmt.Printf("    Non-enc mean lat: %.3f ms\n", report.Aggregate.NonEncMeanLatMs.Mean)
	fmt.Printf("    Enc P99 lat:      %.3f ms\n", report.Aggregate.EncP99LatMs.Mean)
	fmt.Printf("    Non-enc P99 lat:  %.3f ms\n", report.Aggregate.NonEncP99LatMs.Mean)
	fmt.Println("============================================================")

	return report, nil
}

type workerResult struct {
	podLatencies      []PodLatency
	filterPassCounts  []int
	scheduled         int
	failed            int
	queueReuseCount   int
	migrationCount    int
	bindConflicts     int
	bindOtherErrors   int
	shadowChecked     int
	misplaced         int
	reserveRejects    int
	gateRejects       int
	changedDuringBind int
	auditErrors       int
	reuseFallbacks    int
	homeCopiesLeft    int
	staleRankings     int
	violations        scheduler.ViolationCounter
	regretSum         float64
	regretMax         float64
	regretRelSum      float64
	regretRelMax      float64
	shadowNotPassing  int
	shadowRaced       int
}

func groupNodesByCluster(nodes []types.Node) map[string][]types.Node {
	clusters := make(map[string][]types.Node)
	for _, n := range nodes {
		c := n.Cluster
		if c == "" {
			c = "cluster0"
		}
		clusters[c] = append(clusters[c], n)
	}
	return clusters
}

func clusterNames(clusterMap map[string][]types.Node) []string {
	names := make([]string, 0, len(clusterMap))
	for k := range clusterMap {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

type sharedRanking struct {
	mu       sync.Mutex
	list     []scheduler.RankedNode
	forPod   *types.Pod
	priority string

	recheckGates bool

	id        uint64
	rankedFor string
	excluded  scheduler.ExclusionCounts
}

type rankingSet struct {
	recheck bool
	single  *sharedRanking
	mu      sync.Mutex
	byClass map[rankingClass]*sharedRanking

	lastResidency *sharedRanking
	ids           atomic.Uint64
}

const DefaultReuseClasses = true

const DefaultRecheckGates = true

type rankingClass struct {
	priority, home string
	origin         types.Region
	gdpr, iso, enc bool
}

func newRankingSet(recheck, classes bool) *rankingSet {
	rs := &rankingSet{recheck: recheck}
	if classes {
		rs.byClass = map[rankingClass]*sharedRanking{}
	} else {
		rs.single = &sharedRanking{recheckGates: recheck}
	}
	return rs
}

func (rs *rankingSet) pick(priority string, pod types.Pod) *sharedRanking {
	if rs.single != nil {
		return rs.single
	}
	k := rankingClass{priority: priority, home: pod.HomeCluster, origin: pod.DataRegion,
		gdpr: pod.GDPRRequired, iso: pod.IsolationRequired, enc: pod.EncryptionRequired}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	s, ok := rs.byClass[k]
	if !ok {
		s = &sharedRanking{recheckGates: rs.recheck}
		rs.byClass[k] = s
	}
	if pod.GDPRRequired {
		rs.lastResidency = s
	}
	return s
}

func (rs *rankingSet) residencyTarget() *sharedRanking {
	if rs.single != nil {
		return rs.single
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.lastResidency
}

func (rs *rankingSet) nextID() uint64 { return rs.ids.Add(1) }

func (s *sharedRanking) take(tracker *scheduler.NodeResourceTracker, pod types.Pod, rejects, gateRejects *int) (types.Node, bool) {
	for len(s.list) > 0 {
		cand := s.list[0].Node
		cur := scheduler.CurrentNode(cand)
		if s.recheckGates && !scheduler.PassesGates(pod, cur) {
			s.list = s.list[1:]
			*gateRejects++
			continue
		}
		if tracker.Reserve(cand.Name, pod, cand) {
			return cur, true
		}
		s.list = s.list[1:]
		*rejects++
	}
	return types.Node{}, false
}

func (s *sharedRanking) reusableFor(pod types.Pod, priority string) bool {
	return s.forPod != nil && len(s.list) > 0 && s.priority == priority &&
		s.forPod.HomeCluster == pod.HomeCluster && s.forPod.DataRegion == pod.DataRegion &&
		scheduler.CanReuseQueue(*s.forPod, pod)
}

func (s *sharedRanking) peek(tracker *scheduler.NodeResourceTracker, pod types.Pod) (types.Node, bool) {
	for _, r := range s.list {
		if tracker.Admits(r.Node.Name, pod, r.Node) {
			return r.Node, true
		}
	}
	return types.Node{}, false
}

func executeRun(runIndex int, clientset kubernetes.Interface, cs *clusterSet,
	tracker *scheduler.NodeResourceTracker, nodes []types.Node,
	ahpWeights scheduler.AHPWeights, cfg Config, podCh <-chan generators.PodInfo) RunMetrics {
	rm := RunMetrics{
		RunIndex:         runIndex,
		TotalPods:        cfg.Pods,
		TotalNodes:       len(nodes),
		Schedulers:       cfg.Schedulers,
		PodLatencies:     make([]PodLatency, 0, cfg.Pods),
		FilterPassCounts: make([]int, 0, cfg.Pods),
	}

	clusterMap := groupNodesByCluster(nodes)
	clusterList := clusterNames(clusterMap)
	multiCluster := len(clusterList) > 1

	rankings := newRankingSet(cfg.RecheckGates, cfg.ReuseClasses)

	rankContext := func(pod types.Pod) scheduler.RankContext {
		rc := scheduler.RankContext{Scorer: cfg.Scorer, Pod: &pod, Usage: tracker,
			CompliantLast: cfg.KeepCompliant && !pod.GDPRRequired}
		if multiCluster {
			rc.DataRegion, rc.HomeCluster, rc.MovePenalty = pod.DataRegion, pod.HomeCluster, cfg.MovePenalty
		}
		return rc
	}

	filterRank := func(pod types.Pod, priority string) ([]scheduler.RankedNode, int, time.Duration, time.Duration, scheduler.ExclusionCounts) {
		var excluded scheduler.ExclusionCounts
		fStart := time.Now()
		if scheduler.Live == nil {
			idx := scheduler.FilterIndices(nodes, pod, tracker, &excluded)
			fDur := time.Since(fStart)
			if len(idx) == 0 {
				return nil, 0, fDur, 0, excluded
			}
			rStart := time.Now()
			ranked := scheduler.RankIndexed(nodes, idx, ahpWeights, priority, rankContext(pod))
			return ranked, len(idx), fDur, time.Since(rStart), excluded
		}

		filtered := scheduler.FilterNodesCounted(nodes, pod, tracker, &excluded)
		fDur := time.Since(fStart)
		if len(filtered) == 0 {
			return nil, 0, fDur, 0, excluded
		}
		rStart := time.Now()
		ranked := scheduler.RankNodesFor(filtered, ahpWeights, priority, rankContext(pod))
		return ranked, len(filtered), fDur, time.Since(rStart), excluded
	}

	if multiCluster {
		fmt.Printf("  Multi-cluster mode: %d clusters [", len(clusterList))
		for i, c := range clusterList {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%s(%d nodes)", c, len(clusterMap[c]))
		}
		fmt.Printf("], every cluster ranked together, move penalty %.2f of the weight sum\n", cfg.MovePenalty)
	}

	sysMonitor := NewSystemMonitor()
	ctx, cancel := context.WithCancel(context.Background())
	go sysMonitor.Run(ctx, 100*time.Millisecond)

	rm.StartTime = time.Now()

	resultCh := make(chan workerResult, cfg.Schedulers)
	var wg sync.WaitGroup

	for w := 0; w < cfg.Schedulers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			workerRng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			currentBatchCluster := clusterList[workerRng.Intn(len(clusterList))]
			batchRemaining := 3 + workerRng.Intn(8)

			wr := workerResult{
				podLatencies:     make([]PodLatency, 0, cfg.Pods/cfg.Schedulers+1),
				filterPassCounts: make([]int, 0, cfg.Pods/cfg.Schedulers+1),
			}

			for pi := range podCh {
				pod := pi.ToPod()

				podPriority := cfg.Priority
				if pod.SchedulingPriority != "" {
					podPriority = pod.SchedulingPriority
				}

				if batchRemaining <= 0 {
					currentBatchCluster = clusterList[workerRng.Intn(len(clusterList))]
					batchRemaining = 3 + workerRng.Intn(8)
				}
				sourceCluster := currentBatchCluster
				batchRemaining--
				if cs.federated() && pi.Cluster != "" {
					sourceCluster = pi.Cluster
				}
				if multiCluster {
					pod.HomeCluster = sourceCluster
					pod.DataRegion = cs.regionOf(sourceCluster)
				}

				podStart := time.Now()

				var filterDur, rankDur, bindDur, migrationDur time.Duration
				migrated := false
				targetCluster := sourceCluster
				var chosen types.Node
				var rec decisionRecord
				gotNode := false

				shared := rankings.pick(podPriority, pod)
				listRanOut := false
				shared.mu.Lock()
				if !cfg.DisableReuse && shared.reusableFor(pod, podPriority) {
					if cfg.ShadowRate > 0 && workerRng.Float64() < cfg.ShadowRate {
						if next, ok := shared.peek(tracker, pod); ok {
							if sr, ok := scheduler.ShadowEvaluate(nodes, pod, tracker,
								ahpWeights, podPriority, rankContext(pod), next.Name); ok && sr.Raced {
								wr.shadowRaced++
							} else if ok {
								wr.shadowChecked++
								if sr.Misplaced {
									wr.misplaced++
								}
								regret := sr.BestScore - sr.ChosenScore
								if regret < 0 {
									regret = 0
								}
								wr.regretSum += regret
								if regret > wr.regretMax {
									wr.regretMax = regret
								}
								if sr.BestScore > 0 {
									rel := regret / sr.BestScore
									wr.regretRelSum += rel
									if rel > wr.regretRelMax {
										wr.regretRelMax = rel
									}
								}
								if sr.NotPassing {
									wr.shadowNotPassing++
								}
							}
						}
					}
					drops0 := wr.gateRejects
					chosen, gotNode = shared.take(tracker, pod, &wr.reserveRejects, &wr.gateRejects)
					if gotNode {
						wr.queueReuseCount++
						p := pod
						shared.forPod = &p
						rec = decisionRecord{path: "reused", rankingID: shared.id, rankedFor: shared.rankedFor,
							excluded: shared.excluded, drops: wr.gateRejects - drops0}
					} else {
						listRanOut = true
					}
				}
				shared.mu.Unlock()

				if !gotNode {
					if listRanOut {
						wr.reuseFallbacks++
					}
					for attempt := 0; attempt < 2 && !gotNode; attempt++ {
						ranked, passed, fDur, rDur, excluded := filterRank(pod, podPriority)
						filterDur += fDur
						rankDur += rDur
						rec = decisionRecord{path: "fresh", excluded: excluded}
						if attempt == 0 {
							wr.filterPassCounts = append(wr.filterPassCounts, passed)
						}
						if passed == 0 {
							break
						}
						shared.mu.Lock()
						p := pod
						shared.list, shared.forPod, shared.priority = ranked, &p, podPriority
						shared.id = rankings.nextID()
						shared.rankedFor, shared.excluded = pi.Namespace+"/"+pi.Name, excluded
						drops0 := wr.gateRejects
						chosen, gotNode = shared.take(tracker, pod, &wr.reserveRejects, &wr.gateRejects)
						if gotNode {
							rec = decisionRecord{path: "fresh", rankingID: shared.id, rankedFor: shared.rankedFor,
								excluded: excluded, drops: wr.gateRejects - drops0}
						}
						shared.mu.Unlock()
						if !gotNode {
							wr.staleRankings++
						}
					}
				}

				if !gotNode {
					wr.failed++

					if cfg.audit != nil {
						if err := cfg.audit.Append(rec.unschedulable(workerID, pi, pod), true); err != nil {
							wr.auditErrors++
						}
					}
					podEnd := time.Now()
					wr.podLatencies = append(wr.podLatencies, PodLatency{
						TotalNs:            podEnd.Sub(podStart).Nanoseconds(),
						FilterNs:           filterDur.Nanoseconds(),
						RankNs:             rankDur.Nanoseconds(),
						EncryptionRequired: pod.EncryptionRequired,
						ResidencyRequired:  pod.GDPRRequired,
						SourceCluster:      sourceCluster,
					})
					continue
				}

				bestNode := chosen.Name
				bestNodeCluster := chosen.Cluster
				if bestNodeCluster == "" {
					bestNodeCluster = "cluster0"
				}
				targetCluster = bestNodeCluster
				if targetCluster != sourceCluster {
					migrated = true
				}

				var auditErr error
				if cfg.audit != nil {
					if err := cfg.audit.Append(rec.entry(workerID, pi, pod, chosen), true); err != nil {
						wr.auditErrors++
						if cfg.audit.Mode() == auditlog.Sync {
							auditErr = err
						}
					}
				}

				bindStart := time.Now()
				var bindErr error
				switch {
				case auditErr != nil:
					bindErr = fmt.Errorf("decision not recorded, not bound: %w", auditErr)
				case cs.federated() && targetCluster != sourceCluster:
					bindErr = migratePod(cs.clients[sourceCluster], cs.clients[targetCluster],
						pi.Name, pi.Namespace, bestNode)

					migrationDur = time.Since(bindStart)
				case cs.federated():
					bindErr = generators.BindPod(cs.clients[targetCluster], pi.Name, pi.Namespace, bestNode)
				default:
					bindErr = generators.BindPod(clientset, pi.Name, pi.Namespace, bestNode)
				}
				bindDur = time.Since(bindStart)

				if errors.Is(bindErr, errHomeCopyLeft) {
					wr.homeCopiesLeft++
					bindErr = nil
				}
				if bindErr != nil {
					wr.failed++

					if apierrors.IsConflict(bindErr) || apierrors.IsAlreadyExists(bindErr) {
						wr.bindConflicts++
					} else {
						wr.bindOtherErrors++
					}
					tracker.Release(bestNode, pod)
				} else {
					wr.scheduled++

					wr.violations.Record(pod, chosen)
					outcome := "bound"
					if scheduler.Live != nil && len(scheduler.CheckViolations(pod, chosen)) == 0 &&
						len(scheduler.CheckViolations(pod, scheduler.CurrentNode(chosen))) > 0 {
						wr.changedDuringBind++
						outcome = "changed_during_bind"
					}
					if migrated {
						wr.migrationCount++
					}
					if cfg.audit != nil {
						cfg.audit.Append(outcomeEntry(workerID, pi, pod, bestNode, outcome), false)
					}
				}
				if bindErr != nil && cfg.audit != nil {
					cfg.audit.Append(outcomeEntry(workerID, pi, pod, bestNode, "failed"), false)
				}

				podEnd := time.Now()
				wr.podLatencies = append(wr.podLatencies, PodLatency{
					TotalNs:            podEnd.Sub(podStart).Nanoseconds(),
					FilterNs:           filterDur.Nanoseconds(),
					RankNs:             rankDur.Nanoseconds(),
					BindNs:             bindDur.Nanoseconds(),
					EncryptionRequired: pod.EncryptionRequired,
					ResidencyRequired:  pod.GDPRRequired,
					Migrated:           migrated,
					SourceCluster:      sourceCluster,
					TargetCluster:      targetCluster,
					MigrationNs:        migrationDur.Nanoseconds(),
					Bound:              bindErr == nil,
				})
			}

			resultCh <- wr
		}(w)
	}

	var churn *labelChurn
	churnCancel := func() {}
	var churnDone chan struct{}
	if cfg.LabelChurnMs > 0 && scheduler.Live != nil {
		seed := int64(0)
		if cfg.Seed != 0 {
			seed = cfg.Seed*31 + int64(runIndex)
		}
		churn = newLabelChurn(clientset, scheduler.Live, time.Duration(cfg.LabelChurnMs)*time.Millisecond,
			cfg.LabelChurnTarget, rankings, nodes, seed)
		churnCtx, cancelChurn := context.WithCancel(context.Background())
		churnCancel = cancelChurn
		churnDone = make(chan struct{})
		go func() {
			churn.run(churnCtx)
			close(churnDone)
		}()
	}

	wg.Wait()
	churnCancel()
	if churnDone != nil {
		<-churnDone
	}
	close(resultCh)

	rm.EndTime = time.Now()

	if churn != nil {
		n, err := churn.restore(2 * time.Minute)
		rm.LabelChanges = n
		rm.LabelPatchErrors = churn.patchErrors()
		fmt.Printf("  Label churn: %d node(s) moved out of Europe during the run, all put back\n", n)
		if err != nil {
			fmt.Printf("  WARNING: %v\n", err)
		}
	}

	var violations scheduler.ViolationCounter
	var shadowChecked, misplaced int
	var regretSum, regretRelSum, regretMax, regretRelMax float64
	var shadowNotPassing, shadowRaced int
	for wr := range resultCh {
		rm.PodLatencies = append(rm.PodLatencies, wr.podLatencies...)
		rm.FilterPassCounts = append(rm.FilterPassCounts, wr.filterPassCounts...)
		rm.Scheduled += wr.scheduled
		rm.Failed += wr.failed
		rm.QueueReuseCount += wr.queueReuseCount
		rm.MigrationCount += wr.migrationCount
		rm.BindConflicts += wr.bindConflicts
		rm.BindOtherErrors += wr.bindOtherErrors
		rm.ReserveRejects += wr.reserveRejects
		rm.GateRejects += wr.gateRejects
		rm.ChangedDuringBind += wr.changedDuringBind
		rm.AuditErrors += wr.auditErrors
		rm.ReuseFallbacks += wr.reuseFallbacks
		rm.HomeCopiesLeft += wr.homeCopiesLeft
		rm.StaleRankings += wr.staleRankings
		violations.Merge(wr.violations)
		shadowChecked += wr.shadowChecked
		misplaced += wr.misplaced
		regretSum += wr.regretSum
		regretRelSum += wr.regretRelSum
		if wr.regretMax > regretMax {
			regretMax = wr.regretMax
		}
		if wr.regretRelMax > regretRelMax {
			regretRelMax = wr.regretRelMax
		}
		shadowNotPassing += wr.shadowNotPassing
		shadowRaced += wr.shadowRaced
	}

	pq := scheduler.ComputePlacementQuality(nodes, tracker, false)
	pq.ShadowChecked = shadowChecked
	pq.Misplaced = misplaced
	pq.ScoreRegretMax = regretMax
	pq.ScoreRegretRelMax = regretRelMax
	pq.ShadowNotPassing = shadowNotPassing
	pq.ShadowRaced = shadowRaced
	if shadowChecked > 0 {
		pq.MisplacementRate = float64(misplaced) / float64(shadowChecked)
		pq.ScoreRegret = regretSum / float64(shadowChecked)
		pq.ScoreRegretRel = regretRelSum / float64(shadowChecked)
	}
	rm.Placement = &pq
	vr := violations.Report()
	rm.Violations = &vr

	cancel()
	time.Sleep(10 * time.Millisecond)
	rm.SystemStats = sysMonitor.Samples()

	return rm
}

func buildMigrationSummary(podLats []PodLatency, migCount, scheduled int) *MigrationSummary {
	ms := &MigrationSummary{
		TotalMigrations: migCount,
		ClusterFlows:    make(map[string]int),
	}
	if scheduled > 0 {
		ms.MigrationRate = float64(migCount) / float64(scheduled) * 100
	}

	var migrationNsSlice []float64
	var localLatNs, migratedLatNs []float64
	perHome := make(map[string][]float64)
	perHomeMigrated := make(map[string]int)
	perHomeResidency := make(map[string]int)
	perHomeResidencyMigrated := make(map[string]int)

	for _, pl := range podLats {
		if pl.SourceCluster != "" && pl.Bound {
			perHome[pl.SourceCluster] = append(perHome[pl.SourceCluster], float64(pl.TotalNs)/1e6)
			if pl.Migrated {
				perHomeMigrated[pl.SourceCluster]++
			}
			if pl.ResidencyRequired {
				perHomeResidency[pl.SourceCluster]++
				if pl.Migrated {
					perHomeResidencyMigrated[pl.SourceCluster]++
				}
			}
		}
		if pl.Migrated {
			migMs := float64(pl.MigrationNs) / 1e6
			migrationNsSlice = append(migrationNsSlice, migMs)
			migratedLatNs = append(migratedLatNs, float64(pl.TotalNs)/1e6)
			if pl.SourceCluster != "" && pl.TargetCluster != "" {
				flow := pl.SourceCluster + "->" + pl.TargetCluster
				ms.ClusterFlows[flow]++
			}
		} else if pl.TotalNs > 0 {
			localLatNs = append(localLatNs, float64(pl.TotalNs)/1e6)
		}
	}

	if len(migrationNsSlice) > 0 {
		sort.Float64s(migrationNsSlice)
		var sum float64
		for _, v := range migrationNsSlice {
			sum += v
		}
		ms.MeanMigrationMs = sum / float64(len(migrationNsSlice))
		ms.P50MigrationMs = Percentile(migrationNsSlice, 50)
		ms.P95MigrationMs = Percentile(migrationNsSlice, 95)
		ms.P99MigrationMs = Percentile(migrationNsSlice, 99)
	}
	if len(localLatNs) > 0 {
		var sum float64
		for _, v := range localLatNs {
			sum += v
		}
		ms.MeanLocalLatMs = sum / float64(len(localLatNs))
	}
	if len(migratedLatNs) > 0 {
		var sum float64
		for _, v := range migratedLatNs {
			sum += v
		}
		ms.MeanMigratedLatMs = sum / float64(len(migratedLatNs))
	}
	if len(perHome) > 0 {
		ms.PerHomeCluster = make(map[string]HomeClusterStats, len(perHome))
		for home, lats := range perHome {
			sort.Float64s(lats)
			var sum float64
			for _, v := range lats {
				sum += v
			}
			ms.PerHomeCluster[home] = HomeClusterStats{
				Pods:     len(lats),
				Migrated: perHomeMigrated[home],

				ResidencyPods:     perHomeResidency[home],
				ResidencyMigrated: perHomeResidencyMigrated[home],
				MeanLatMs:         sum / float64(len(lats)),
				P50LatMs:          Percentile(lats, 50),
				P99LatMs:          Percentile(lats, 99),
			}
		}
	}

	return ms
}

func bookExistingPods(clientset kubernetes.Interface, tracker *scheduler.NodeResourceTracker, nodes []types.Node, ownNamespace string) (int, error) {
	known := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		known[n.Name] = true
	}
	list, err := clientset.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	booked := 0
	for i := range list.Items {
		p := &list.Items[i]
		if p.Spec.NodeName == "" || !known[p.Spec.NodeName] || p.Namespace == ownNamespace {
			continue
		}
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		var milli, mb int64
		for _, c := range p.Spec.Containers {
			milli += c.Resources.Requests.Cpu().MilliValue()
			mb += c.Resources.Requests.Memory().Value() / (1024 * 1024)
		}
		if milli == 0 && mb == 0 {
			continue
		}
		tracker.Update(p.Spec.NodeName, types.Pod{Name: p.Name, CPU: uint32(milli), Memory: uint32(mb)})
		booked++
	}
	return booked, nil
}

func ensureNamespace(clientset kubernetes.Interface, namespace string) error {
	ctx := context.Background()

	_, err := clientset.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err == nil {
		fmt.Printf("  Namespace '%s' exists\n", namespace)
		return nil
	}

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	_, err = clientset.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if err != nil {
		_, getErr := clientset.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("creating namespace %s: %w", namespace, err)
		}
	}
	fmt.Printf("  Created namespace '%s'\n", namespace)
	return nil
}

func scorerOrDefault(s scheduler.Scorer) scheduler.Scorer {
	if s == "" {
		return scheduler.ScorerAHP
	}
	return s
}

func feedFromQueue(pq PodQueue, want, workers int) <-chan generators.PodInfo {
	buf := workers * 2
	if buf < 8 {
		buf = 8
	}
	out := make(chan generators.PodInfo, buf)
	feeders := workers
	if feeders < 1 {
		feeders = 1
	}
	if feeders > 16 {
		feeders = 16
	}

	var delivered int64
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < feeders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if atomic.AddInt64(&delivered, 1) > int64(want) {
					return
				}
				pod, ok, err := pq.Pop(ctx)
				if err != nil || !ok {
					return
				}
				select {
				case out <- pod:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(out)
		cancel()
		pq.Close()
	}()
	return out
}

func (c Config) paced() bool {
	return c.ArrivalBatch > 0 && c.ArrivalIntervalMs > 0
}

func (c Config) OfferedRate() float64 {
	if !c.paced() {
		return 0
	}
	return float64(c.ArrivalBatch) * 1000 / float64(c.ArrivalIntervalMs)
}
