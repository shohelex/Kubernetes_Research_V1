# Kubernetes_Research_V1

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.23737115.svg)](https://doi.org/10.5281/zenodo.23737115)

Code for our paper on enforcing data residency, isolation and encryption when scheduling pods across Kubernetes clusters. The scheduler drops every node of every cluster that does not meet a pod's requirements before it ranks anything, checks the node's labels again right before binding, and can write each decision to a hash-chained log.

## Layout

- `cmd/benchmark` - the scheduler and its benchmark (creates pods, schedules them with N instances, writes a JSON report)
- `cmd/compare` - runs the same pods through kube-scheduler, YuniKorn, Volcano or Karmada
- `cmd/auditverify` - verifies a decision log, optionally against the pods in the cluster
- `cmd/affinitywebhook` - mutating webhook that adds the equivalent required node affinity to pods
- `pkg/` - the scheduler itself (`scheduler`), the run loop and multi-cluster code (`benchmark`), node/pod generators, the log, types

## Build

Needs Go 1.21+.

```
go build -o bin/ ./cmd/...
go test ./...
```

## Running

Most runs used local [KWOK](https://kwok.sigs.k8s.io/) clusters (kwokctl 0.7, Kubernetes 1.33); see below for real clusters. The tools look for `~/.kwok/clusters/gated-bench/kubeconfig.yaml` by default:

```
kwokctl create cluster --name gated-bench --runtime binary \
  --extra-args kube-scheduler=kube-api-qps=5000 \
  --extra-args kube-scheduler=kube-api-burst=10000
```

The extra args matter: kube-scheduler's default client limit (50/s) caps its throughput, and our tools run at 5000/10000.

Our scheduler, 2000 pods on 500 nodes, half of them with requirements:

```
bin/benchmark -nodes 500 -pods 2000 -privacy-ratio 0.5 -seed 101 \
  -informer-timing -arrival-batch 2000 -arrival-interval-ms 1 -output ours.json
```

kube-scheduler on the same pods (same seed), with the required node affinity on each pod:

```
bin/compare -skip-nodes -pods 2000 -privacy-ratio 0.5 -seed 101 -timeout 900 -output kube.json
```

`-privacy-affinity=false` drops the affinity, `-privacy-preferred` makes it a preference, and `-scheduler yunikorn` or `-scheduler volcano` hands the pods to those schedulers.

Pods carry their requirements as annotations (`placement-gates/gdpr-required`, `placement-gates/isolation-required`, `placement-gates/encryption-required`). Nodes are described by the labels `region`, `isolation`, `encrypted_storage` and `encrypted_traffic`; the generated KWOK nodes have all of them.

### Several clusters

```
bin/benchmark -kubeconfigs eu=eu.yaml,us=us.yaml,asia=asia.yaml,other=other.yaml \
  -cluster-regions "eu=Europe,us=United States,asia=Asia,other=Other" \
  -nodes 125 -pods 2000 -privacy-ratio 0.5 -seed 101 -informer-timing \
  -arrival-batch 2000 -arrival-interval-ms 1 -output federation.json
```

Each pod starts in a home cluster; if the chosen node is elsewhere, the pod is recreated there. `-move-penalty` controls how much home is preferred and `-keep-compliant-capacity` keeps European nodes for the pods that need them.

### Real clusters

The multi-cluster command above also works against real API servers. The nodes are still KWOK nodes, so install the KWOK controller in each cluster first:

```
kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/latest/download/kwok.yaml
kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/latest/download/stage-fast.yaml
```

We used two federations (AWS and Hetzner Cloud) of three kubeadm clusters each (Kubernetes 1.29), one cluster per region, with the benchmark running on the US cluster and `-e2e-timing -reuse-classes=false`; the Hetzner one also had `-move-penalty 0.1`.

To schedule onto a cluster's own kubelet-backed nodes instead:

```
bin/benchmark -kubeconfig real.yaml -real-nodes -region-mode uniform \
  -pods 150 -runs 5 -privacy-ratio 0.5 -output real.json
```

`-real-nodes` labels the schedulable nodes (regions and security properties; `uniform` spreads the regions evenly), reads their capacity from `status.allocatable`, counts the pods already running on them, and creates small `pause` pods that actually run.

### Decision log

```
bin/benchmark ... -audit-log /tmp/decisions -audit-mode sync
bin/auditverify -log /tmp/decisions.run1.jsonl -kubeconfig ~/.kwok/clusters/gated-bench/kubeconfig.yaml
```

Run the verifier before the next run, since the benchmark clears the namespace at the start of each run.

### Other experiments

- `-scorer ahp|equal|k8s|adversarial` swaps the ranking; `-disable-privacy-filter` turns the gates off.
- `-label-churn-ms 50` moves a European node out of Europe every 50 ms during the run; `-recheck-gates=false` and `-disable-reuse` switch off the re-check and ranking reuse.
- `affinitywebhook -crash-after N` stops the webhook after N admissions (needs `-cert`/`-key` and a `MutatingWebhookConfiguration` pointing at it).
- `compare -karmada-members eu=eu.yaml,...` with `-kubeconfig` set to the Karmada API server runs the pods through Karmada (we used v1.19.0). Pods with the residency requirement get the label `residency=required` for your propagation policies.

## Results

`results/` has the reports of the live-cluster runs (September 2026, five runs each): `aws_federation.json` and `hetzner_federation.json` (2000 pods per run across three clusters, about half of them residency-bound) and `real_nodes.json` (150 pods per run on four kubelet-backed workers). They were written by an earlier revision of the benchmark, so a few config fields differ from what this version writes; the AWS run, for example, used a Redis pod queue.

## License

Apache 2.0, see `LICENSE`.
