package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/benchmark"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
)

func main() {
	home, _ := os.UserHomeDir()

	pods := flag.Int("pods", 2000, "number of pods")
	nodes := flag.Int("nodes", 500, "number of nodes (per cluster with -kubeconfigs)")
	clusters := flag.Int("clusters", 1, "number of clusters simulated on one API server")
	runs := flag.Int("runs", 1, "number of runs")
	priority := flag.String("priority", "balanced", "balanced, privacy or performance")
	output := flag.String("output", "benchmark_results.json", "report file")
	kubeconfig := flag.String("kubeconfig", filepath.Join(home, ".kwok", "clusters", "gated-bench", "kubeconfig.yaml"), "kubeconfig")
	kubeconfigs := flag.String("kubeconfigs", "", "name=kubeconfig,... to schedule across several clusters")
	clusterRegions := flag.String("cluster-regions", "", "name=region,... for -kubeconfigs")
	movePenalty := flag.Float64("move-penalty", scheduler.DefaultMovePenalty, "score penalty for nodes outside a pod's home cluster")
	skipNodes := flag.Bool("skip-node-creation", false, "reuse the nodes already in the cluster")
	namespace := flag.String("namespace", "gated-bench", "namespace for the pods")
	schedulers := flag.Int("schedulers", 10, "number of scheduler instances")
	informerTiming := flag.Bool("informer-timing", false, "time bindings from an informer")
	e2eTiming := flag.Bool("e2e-timing", false, "include pod creation in the timing")
	realNodes := flag.Bool("real-nodes", false, "use the cluster's own nodes instead of KWOK nodes")
	regionMode := flag.String("region-mode", "weighted", "region labels for -real-nodes: weighted or uniform")
	podProfile := flag.String("pod-profile", "", "real or synthetic pod sizes (default real with -real-nodes)")
	seed := flag.Int64("seed", 0, "random seed (0 uses the clock)")
	privacyRatio := flag.Float64("privacy-ratio", 0, "fraction of pods with privacy requirements")
	scorerName := flag.String("scorer", "ahp", "ahp, equal, k8s or adversarial")
	noGates := flag.Bool("disable-privacy-filter", false, "turn the gates off")
	noReuse := flag.Bool("disable-reuse", false, "rank afresh for every pod")
	reuseClasses := flag.Bool("reuse-classes", benchmark.DefaultReuseClasses, "one shared ranking per requirement class")
	recheck := flag.Bool("recheck-gates", benchmark.DefaultRecheckGates, "check the gates again before binding")
	keepCompliant := flag.Bool("keep-compliant-capacity", false, "rank European nodes last for pods without a residency requirement")
	utilLimit := flag.Float64("utilization-limit", scheduler.UtilizationLimit, "highest node utilization that still takes a pod")
	shadowRate := flag.Float64("shadow-rate", 0, "fraction of reused rankings to compare with a fresh one")
	qps := flag.Float64("client-qps", 5000, "client rate limit")
	burst := flag.Int("client-burst", 10000, "client burst")
	batch := flag.Int("arrival-batch", 0, "pods per arrival group (0: all at once)")
	interval := flag.Int("arrival-interval-ms", 0, "ms between arrival groups")
	churnMs := flag.Int("label-churn-ms", 0, "move a European node out of Europe every N ms")
	churnTarget := flag.String("label-churn-target", "random", "random or top")
	auditLog := flag.String("audit-log", "", "decision log path prefix")
	auditMode := flag.String("audit-mode", "sync", "sync or async")
	flag.Parse()

	if *utilLimit <= 0 || *utilLimit > 1 {
		fail("-utilization-limit must be in (0, 1]")
	}
	scheduler.UtilizationLimit = *utilLimit
	if *priority != "balanced" && *priority != "privacy" && *priority != "performance" {
		fail("-priority must be balanced, privacy or performance")
	}
	if *privacyRatio < 0 || *privacyRatio > 1 {
		fail("-privacy-ratio must be between 0 and 1")
	}
	if *churnTarget != "random" && *churnTarget != "top" {
		fail("-label-churn-target must be random or top")
	}
	if *podProfile != "" && *podProfile != "real" && *podProfile != "synthetic" {
		fail("-pod-profile must be real or synthetic")
	}
	if *regionMode != "weighted" && *regionMode != "uniform" {
		fail("-region-mode must be weighted or uniform")
	}
	if *auditMode != "sync" && *auditMode != "async" {
		fail("-audit-mode must be sync or async")
	}
	scorer, err := scheduler.ParseScorer(*scorerName)
	if err != nil {
		fail(err.Error())
	}

	members, order := pairs(*kubeconfigs)
	regions, _ := pairs(*clusterRegions)
	if len(regions) == 0 {
		regions = nil
	}

	cfg := benchmark.Config{
		Pods:                 *pods,
		Nodes:                *nodes,
		Clusters:             *clusters,
		Runs:                 *runs,
		Priority:             *priority,
		OutputPath:           *output,
		Kubeconfig:           *kubeconfig,
		ClusterKubeconfigs:   members,
		ClusterOrder:         order,
		ClusterRegions:       regions,
		MovePenalty:          *movePenalty,
		SkipNodeCreation:     *skipNodes,
		Namespace:            *namespace,
		Schedulers:           *schedulers,
		InformerTiming:       *informerTiming,
		E2ETiming:            *e2eTiming,
		RealNodes:            *realNodes,
		RegionMode:           *regionMode,
		PodProfile:           *podProfile,
		Seed:                 *seed,
		DisableReuse:         *noReuse,
		PrivacyRatio:         *privacyRatio,
		ShadowRate:           *shadowRate,
		DisablePrivacyFilter: *noGates,
		ClientQPS:            float32(*qps),
		ClientBurst:          *burst,
		ArrivalBatch:         *batch,
		ArrivalIntervalMs:    *interval,
		RecheckGates:         *recheck,
		LabelChurnMs:         *churnMs,
		LabelChurnTarget:     *churnTarget,
		AuditLog:             *auditLog,
		AuditMode:            *auditMode,
		Scorer:               scorer,
		ReuseClasses:         *reuseClasses,
		KeepCompliant:        *keepCompliant,
	}

	report, err := benchmark.Run(cfg)
	if err != nil {
		fail(err.Error())
	}
	if err := benchmark.WriteJSON(*output, report); err != nil {
		fail(err.Error())
	}
	fmt.Printf("\nResults written to %s\n", *output)
}

func pairs(v string) (map[string]string, []string) {
	m := map[string]string{}
	var order []string
	for _, kv := range strings.Split(v, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		k, val, ok := strings.Cut(kv, "=")
		if !ok {
			fail(fmt.Sprintf("expected name=value, got %q", kv))
		}
		m[k] = val
		order = append(order, k)
	}
	return m, order
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
