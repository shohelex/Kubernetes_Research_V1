package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/auditlog"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/benchmark"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	logPath := flag.String("log", "", "decision log")
	anchorPath := flag.String("anchor", "", "anchor file (default <log>.anchor)")
	kubeconfig := flag.String("kubeconfig", "", "check the bound pods in this cluster too")
	namespace := flag.String("namespace", "gated-bench", "namespace of the pods")
	output := flag.String("output", "", "also write the report to this file")
	flag.Parse()
	if *logPath == "" {
		fmt.Fprintln(os.Stderr, "auditverify: --log is required")
		os.Exit(2)
	}
	if *anchorPath == "" {
		*anchorPath = *logPath + ".anchor"
	}

	start := time.Now()
	lf, err := os.Open(*logPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer lf.Close()
	af, err := os.Open(*anchorPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "auditverify: no anchor file (%v); the anchors are not checked\n", err)
	}
	var rep auditlog.Report
	var entries []auditlog.Entry
	if af != nil {
		defer af.Close()
		rep, entries = auditlog.Verify(lf, af, benchmark.DecisionCheck)
	} else {
		rep, entries = auditlog.Verify(lf, nil, benchmark.DecisionCheck)
		rep.AnchorOK = false
		rep.AnchorReason = "no anchor file"
	}

	if *kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		client, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		pods, err := client.CoreV1().Pods(*namespace).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		placements := map[string]string{}
		for _, p := range pods.Items {
			if p.Spec.NodeName != "" {
				placements[p.Namespace+"/"+p.Name] = p.Spec.NodeName
			}
		}
		auditlog.CrossCheck(&rep, entries, placements)
	}

	out := struct {
		auditlog.Report
		OK       bool    `json:"ok"`
		VerifyMs float64 `json:"verify_ms"`
	}{rep, rep.OK(), float64(time.Since(start).Microseconds()) / 1000}
	b, _ := json.MarshalIndent(out, "", "  ")
	b = append(b, '\n')
	os.Stdout.Write(b)
	if *output != "" {
		if err := os.WriteFile(*output, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	if !out.OK {
		os.Exit(1)
	}
}
