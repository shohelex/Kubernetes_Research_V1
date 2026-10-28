package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/scheduler"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

func watchNodeLabels(ctx context.Context, clientset kubernetes.Interface, live *scheduler.LiveLabels) {
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = "type=kwok" }),
	)
	update := func(obj interface{}) {
		k, ok := obj.(*corev1.Node)
		if !ok {
			return
		}
		if n, ok := generators.NodeFromK8s(k); ok {
			live.Set(n.Name, scheduler.PrivacyLabelsOf(n))
		}
	}
	factory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    update,
		UpdateFunc: func(_, newObj interface{}) { update(newObj) },
	})
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
}

type labelChurn struct {
	clientset kubernetes.Interface
	live      *scheduler.LiveLabels
	interval  time.Duration
	target    string
	rankings  *rankingSet
	nodes     []types.Node
	rng       *rand.Rand

	mu      sync.Mutex
	changed map[string]types.Region
	errs    int
}

func newLabelChurn(clientset kubernetes.Interface, live *scheduler.LiveLabels, interval time.Duration,
	target string, rankings *rankingSet, nodes []types.Node, seed int64) *labelChurn {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &labelChurn{
		clientset: clientset, live: live, interval: interval, target: target, rankings: rankings,
		nodes: nodes, rng: rand.New(rand.NewSource(seed)), changed: make(map[string]types.Region),
	}
}

func (c *labelChurn) run(ctx context.Context) {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if name, ok := c.pick(); ok {
				c.setRegion(ctx, name, types.RegionUS)
			}
		}
	}
}

func (c *labelChurn) pick() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	eligible := func(name string) bool {
		if _, done := c.changed[name]; done {
			return false
		}
		p, ok := c.live.Get(name)
		return ok && scheduler.GDPRCompliantRegions[p.Region]
	}
	if c.target == "top" && c.rankings != nil {
		if s := c.rankings.residencyTarget(); s != nil {
			s.mu.Lock()
			for _, r := range s.list {
				if eligible(r.Node.Name) {
					s.mu.Unlock()
					return r.Node.Name, true
				}
			}
			s.mu.Unlock()
		}
	}
	var candidates []string
	for _, n := range c.nodes {
		if eligible(n.Name) {
			candidates = append(candidates, n.Name)
		}
	}
	if len(candidates) == 0 {
		return "", false
	}
	return candidates[c.rng.Intn(len(candidates))], true
}

func (c *labelChurn) setRegion(_ context.Context, name string, region types.Region) {
	p, _ := c.live.Get(name)
	c.mu.Lock()
	if _, seen := c.changed[name]; !seen {
		c.changed[name] = p.Region
	}
	c.mu.Unlock()
	patch, _ := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{"labels": map[string]string{"region": generators.RegionLabelValue(region)}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.clientset.CoreV1().Nodes().Patch(ctx, name, k8stypes.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		c.mu.Lock()
		c.errs++
		c.mu.Unlock()
	}
}

func (c *labelChurn) restore(timeout time.Duration) (int, error) {
	c.mu.Lock()
	changed := make(map[string]types.Region, len(c.changed))
	for k, v := range c.changed {
		changed[k] = v
	}
	c.changed = make(map[string]types.Region)
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for name, region := range changed {
		patch, _ := json.Marshal(map[string]interface{}{
			"metadata": map[string]interface{}{"labels": map[string]string{"region": generators.RegionLabelValue(region)}},
		})
		if _, err := c.clientset.CoreV1().Nodes().Patch(ctx, name, k8stypes.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			return len(changed), fmt.Errorf("restoring the region of %s: %w", name, err)
		}
	}
	for name, region := range changed {
		for {
			if p, ok := c.live.Get(name); ok && p.Region == region {
				break
			}
			if ctx.Err() != nil {
				return len(changed), fmt.Errorf("label view did not see %s back in %s", name, region)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return len(changed), nil
}

func (c *labelChurn) patchErrors() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errs
}
