package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/policy"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
)

func patchFor(pod *corev1.Pod) ([]byte, error) {
	aff := policy.PrivacyAffinity(policy.FromAnnotations(pod.Annotations), false)
	if aff == nil || pod.Spec.Affinity != nil {
		return nil, nil
	}
	return json.Marshal([]map[string]interface{}{{"op": "add", "path": "/spec/affinity", "value": aff}})
}

func review(body []byte) ([]byte, bool, error) {
	var ar admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &ar); err != nil || ar.Request == nil {
		return nil, false, fmt.Errorf("bad admission review: %v", err)
	}
	resp := &admissionv1.AdmissionResponse{UID: ar.Request.UID, Allowed: true}
	mutated := false
	var pod corev1.Pod
	if err := json.Unmarshal(ar.Request.Object.Raw, &pod); err == nil {
		if patch, err := patchFor(&pod); err != nil {
			return nil, false, err
		} else if patch != nil {
			pt := admissionv1.PatchTypeJSONPatch
			resp.Patch, resp.PatchType, mutated = patch, &pt, true
		}
	}
	ar.Response, ar.Request = resp, nil
	out, err := json.Marshal(ar)
	return out, mutated, err
}

func main() {
	listen := flag.String("listen", "127.0.0.1:9443", "listen address")
	cert := flag.String("cert", "", "TLS certificate")
	key := flag.String("key", "", "TLS key")
	crashAfter := flag.Int64("crash-after", 0, "stop after this many admissions (0: never)")
	logPath := flag.String("log", "", "log each admission to this file")
	flag.Parse()

	var logMu sync.Mutex
	var logf io.Writer = io.Discard
	if *logPath != "" {
		f, err := os.OpenFile(*logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		logf = f
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var served atomic.Int64
	srv := &http.Server{ReadHeaderTimeout: 5 * time.Second}
	http.HandleFunc("/mutate", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		out, mutated, err := review(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
		n := served.Add(1)
		logMu.Lock()
		fmt.Fprintf(logf, "%s %d mutated=%v\n", time.Now().UTC().Format(time.RFC3339Nano), n, mutated)
		logMu.Unlock()
		if *crashAfter > 0 && n == *crashAfter {
			go func() { ln.Close(); srv.Close() }()
		}
	})
	fmt.Printf("affinitywebhook on %s (crash after %d)\n", *listen, *crashAfter)
	if err := srv.ServeTLS(ln, *cert, *key); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, err)
	}
	fmt.Printf("affinitywebhook stopped after %d admissions\n", served.Load())

	select {}
}
