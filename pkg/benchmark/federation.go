package benchmark

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type clusterSet struct {
	names   []string
	regions map[string]string
	clients map[string]kubernetes.Interface
	setup   map[string]kubernetes.Interface
}

func newClusterSet(cfg Config) (*clusterSet, error) {
	if len(cfg.ClusterKubeconfigs) < 2 {
		return nil, nil
	}
	cs := &clusterSet{
		regions: cfg.ClusterRegions,
		clients: make(map[string]kubernetes.Interface),
		setup:   make(map[string]kubernetes.Interface),
	}
	ctx := context.Background()
	for _, name := range cfg.ClusterOrder {
		path, ok := cfg.ClusterKubeconfigs[name]
		if !ok {
			return nil, fmt.Errorf("cluster %s has no kubeconfig", name)
		}
		rc, err := clientcmd.BuildConfigFromFlags("", path)
		if err != nil {
			return nil, fmt.Errorf("cluster %s: building kubeconfig: %w", name, err)
		}
		rc.QPS = cfg.ClientQPS
		rc.Burst = cfg.ClientBurst
		c, err := kubernetes.NewForConfig(rc)
		if err != nil {
			return nil, fmt.Errorf("cluster %s: client: %w", name, err)
		}
		s, err := kubernetes.NewForConfig(rest.CopyConfig(rc))
		if err != nil {
			return nil, fmt.Errorf("cluster %s: setup client: %w", name, err)
		}
		if _, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
			return nil, fmt.Errorf("cluster %s unreachable through %s: %w", name, path, err)
		}
		if r, ok := cs.regions[name]; ok {
			switch types.Region(r) {
			case types.RegionEurope, types.RegionUS, types.RegionAsia, types.RegionOther:
			default:
				return nil, fmt.Errorf("cluster %s: unknown region %q (Europe, United States, Asia, Other)", name, r)
			}
		}
		cs.names = append(cs.names, name)
		cs.clients[name] = c
		cs.setup[name] = s
	}
	return cs, nil
}

func (cs *clusterSet) federated() bool { return cs != nil && len(cs.names) > 1 }

func (cs *clusterSet) regionOf(cluster string) types.Region {
	if !cs.federated() {
		return ""
	}
	return types.Region(cs.regions[cluster])
}

func (cs *clusterSet) primary() (kubernetes.Interface, kubernetes.Interface) {
	return cs.clients[cs.names[0]], cs.setup[cs.names[0]]
}

func (cs *clusterSet) clusterNames() []string {
	if !cs.federated() {
		return nil
	}
	return append([]string(nil), cs.names...)
}

func (cs *clusterSet) setupClients() map[string]kubernetes.Interface {
	if !cs.federated() {
		return nil
	}
	return cs.setup
}

func (cs *clusterSet) ensureNamespaces(namespace string) error {
	for _, name := range cs.names {
		if err := ensureNamespace(cs.clients[name], namespace); err != nil {
			return fmt.Errorf("cluster %s: %w", name, err)
		}
	}
	return nil
}

func (cs *clusterSet) loadNodes(cfg Config) ([]types.Node, error) {
	var all []types.Node
	for _, name := range cs.names {
		c := cs.clients[name]
		if !cfg.SkipNodeCreation {
			if err := generators.DeleteKWOKNodes(c); err != nil {
				fmt.Printf("  cluster %s: could not clean old nodes: %v\n", name, err)
			}
			time.Sleep(1 * time.Second)
			if err := generators.CreateKWOKNodesNamed(c, cfg.Nodes, 1, "gated-"+name+"-node"); err != nil {
				return nil, fmt.Errorf("cluster %s: creating KWOK nodes: %w", name, err)
			}
		}
		nodes, err := generators.ReadNodesFromK8s(c)
		if err != nil {
			return nil, fmt.Errorf("cluster %s: reading nodes: %w", name, err)
		}
		region := types.Region(cs.regions[name])
		for i := range nodes {
			nodes[i].Cluster = name
			if region != "" {
				nodes[i].Region = region
			}
		}
		where := "generator regions"
		if region != "" {
			where = string(region)
		}
		fmt.Printf("  cluster %s: %d nodes (%s)\n", name, len(nodes), where)
		all = append(all, nodes...)
	}
	return all, nil
}

func (cs *clusterSet) resetPods(namespace string, budget time.Duration) error {
	for _, name := range cs.names {
		if err := generators.ResetPodsWithTimeout(cs.clients[name], namespace, budget); err != nil {
			return fmt.Errorf("cluster %s: %w", name, err)
		}
	}
	return nil
}

func migratePod(src, dst kubernetes.Interface, name, namespace, nodeName string) error {
	ctx := context.Background()
	p, err := src.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("export from source: %w", err)
	}
	travelling := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        p.Name,
			Namespace:   p.Namespace,
			Labels:      p.Labels,
			Annotations: p.Annotations,
		},
		Spec: *p.Spec.DeepCopy(),
	}
	if travelling.Annotations == nil {
		travelling.Annotations = map[string]string{}
	}
	travelling.Annotations["placement-gates/migrated-from"] = p.Labels["placement-gates/home-cluster"]
	travelling.Spec.NodeName = nodeName
	if _, err := dst.CoreV1().Pods(namespace).Create(ctx, travelling, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create at target: %w", err)
	}
	grace := int64(0)
	var derr error
	for attempt := 0; attempt < 3; attempt++ {
		derr = src.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
		if derr == nil || apierrors.IsNotFound(derr) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("%w: %v", errHomeCopyLeft, derr)
}

var errHomeCopyLeft = errors.New("pod created at target, home copy not deleted")
