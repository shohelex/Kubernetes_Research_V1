package scheduler

import (
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
)

func CanReuseQueue(prevPod, currentPod types.Pod) bool {
	return currentPod.CPU <= prevPod.CPU &&
		currentPod.Memory <= prevPod.Memory &&
		currentPod.Storage <= prevPod.Storage &&
		currentPod.Bandwidth <= prevPod.Bandwidth &&
		currentPod.IOSpeed <= prevPod.IOSpeed &&
		currentPod.Latency >= prevPod.Latency &&
		(!currentPod.GDPRRequired || prevPod.GDPRRequired) &&
		(!currentPod.IsolationRequired || prevPod.IsolationRequired) &&
		(!currentPod.EncryptionRequired || prevPod.EncryptionRequired)
}
