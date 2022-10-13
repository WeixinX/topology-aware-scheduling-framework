package scheduler

type appId string

type msId string

type nodeId string

const (
	ResCPU ResourceType = iota
	ResMem
	ResNet
)

const (
	KB = 1 << (10 * (iota + 1))
	MB
	GB
	TB
)

const (
	PrevNull         = "-1" // 在一条路径上，当前节点没有前驱节点
	NotPlaced nodeId = "-1" // 微服务没有放置在任何工作节点上

	AppQIniLen = 20

	AlphaC float32 = 0.33 // argument of the score function for cost
	AlphaI float32 = 0.33 // argument of the score function for inter
	AlphaF float32 = 0.33 // argument of the score function for frag
)
