package types

type Exposure string

const (
	ExposurePrivate Exposure = "private"
	ExposurePublic  Exposure = "public"
)

type Isolation string

const (
	IsolationNone          Isolation = "none"
	IsolationNamespace     Isolation = "namespace"
	IsolationNetworkPolicy Isolation = "network_policy"
	IsolationDedicated     Isolation = "dedicated"
	IsolationVmIsolated    Isolation = "vm_isolated"
)

type Region string

const (
	RegionEurope Region = "Europe"
	RegionUS     Region = "United States"
	RegionAsia   Region = "Asia"
	RegionOther  Region = "Other"
)

type TrafficEncryption struct {
	Dt     float64
	Ispeed float64
	Pl     float64
	Lat    float64
	Jit    float64
}

type Node struct {
	Name             string
	CPUCores         uint32
	Memory           uint32
	Storage          uint32
	Bandwidth        uint32
	IOSpeed          uint32
	Ping             uint32
	LCache           uint32
	Exposure         Exposure
	Isolation        Isolation
	EncryptedStorage *float64
	EncryptedTraffic *TrafficEncryption
	Region           Region
	Cluster          string
}
