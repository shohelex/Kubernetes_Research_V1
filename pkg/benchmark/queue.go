package benchmark

import (
	"context"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/generators"
)

type PodQueue interface {
	Push(pod generators.PodInfo) error

	Pop(ctx context.Context) (generators.PodInfo, bool, error)

	Close() error
	Name() string
}

func NewPodQueue(cfg Config, capacity int) (PodQueue, error) {
	return newMemoryQueue(capacity), nil
}

type memoryQueue struct {
	ch     chan generators.PodInfo
	closed bool
}

func newMemoryQueue(capacity int) *memoryQueue {
	return &memoryQueue{ch: make(chan generators.PodInfo, capacity)}
}

func (q *memoryQueue) Push(pod generators.PodInfo) error {
	q.ch <- pod
	return nil
}

func (q *memoryQueue) Pop(ctx context.Context) (generators.PodInfo, bool, error) {
	select {
	case pod, ok := <-q.ch:
		return pod, ok, nil
	case <-ctx.Done():
		return generators.PodInfo{}, false, ctx.Err()
	}
}

func (q *memoryQueue) Close() error {
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
	return nil
}

func (q *memoryQueue) Name() string { return "in-memory channel" }
