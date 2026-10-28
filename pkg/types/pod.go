package types

type WorkloadType string

const (
	WorkloadLongRunning WorkloadType = "long_running"
	WorkloadBatch       WorkloadType = "batch"
	WorkloadCron        WorkloadType = "cron"
)

type Pod struct {
	Name               string
	Namespace          string
	Cluster            string
	CPU                uint32
	Memory             uint32
	Storage            uint32
	Bandwidth          uint32
	IOSpeed            uint32
	Latency            uint32
	GDPRRequired       bool
	IsolationRequired  bool
	EncryptionRequired bool
	WorkloadType       WorkloadType

	SchedulingPriority string

	HomeCluster string
	DataRegion  Region
}
