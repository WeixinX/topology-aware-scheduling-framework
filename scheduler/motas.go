package scheduler

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

//
// MOTAS framework
//
type MOTAS struct {
	death     uint32
	mu        sync.RWMutex
	app       map[appId]*Service // microservice applications
	cluster   *Cluster           // cluster of worker nodes where the microservice is placed
	scheduleQ *appQueue          // priority queue for scheduling of microservice app
	alphaC    float32            // argument of the score function for cost
	alphaI    float32            // argument of the score function for inter
	alphaF    float32            // argument of the score function for frag
}

func NewMOTAS(cluster *Cluster) *MOTAS {
	mts := &MOTAS{
		mu:        sync.RWMutex{},
		app:       make(map[appId]*Service),
		cluster:   cluster,
		scheduleQ: newAppQueue(AppQIniLen),
		alphaC:    AlphaC,
		alphaI:    AlphaI,
		alphaF:    AlphaF,
	}
	go mts.run()

	return mts
}

func (m *MOTAS) Kill() {
	atomic.StoreUint32(&m.death, 1)
}

func (m *MOTAS) killed() bool {
	return atomic.LoadUint32(&m.death) == 1
}

// Run 运行 MOTAS
func (m *MOTAS) run() {
	var (
		app     *Service
		ms2node map[msId]nodeId
		err     error
	)

	for !m.killed() {
		for !m.scheduleQ.empty() {
			app = m.scheduleQ.pop()
			ms2node, err = m.recursiveMapping(app.id, app.ms, m.cluster)
			if err != nil || len(ms2node) == 0 { // 没能得到一个有效的映射结果，降低该应用调度优先级并重新放入队列（错误类型只有资源不足）
				// 状态回滚
				m.cluster.rollbackAlloc()
				m.cluster.rollbackGama()
				m.cluster.rollbackBandAlloc()
				app.rollbackPlaceStat()
				// 降低优先级并重入队列
				app.decPriority()
				m.scheduleQ.push(app)
			} else {
				m.doPlacement(ms2node) // 根据映射关系将微服务放置到对应的工作节点上
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (m *MOTAS) AddTask(app *Service) {
	m.scheduleQ.push(app)
}

// recursiveMapping 递归求解微服务与工作节点的映射关系
func (m *MOTAS) recursiveMapping(aid appId, mss map[msId]*Microservice, cluster *Cluster) (map[msId]nodeId, error) {
	// return condition
	ms2node := make(map[msId]nodeId)
	if len(mss) == 0 { // 没有需要调度的微服务
		return ms2node, nil
	}
	if cluster.nodeCount() == 1 { // 所有微服务都放置到该节点
		var node *Node
		for _, n := range cluster.nodes { // 取出唯一节点
			node = n
		}
		for _, ms := range mss { // 建立服务和工作节点的映射关系，并预分配资源
			// 服务与节点的映射关系
			m.app[aid].setNextPlaceNode(ms.id, node.id)
			ms2node[ms.id] = node.id
			// 预分配各维度资源并计算资源利用率（以确定资源碎片）
			for _, res := range ms.resReq {
				cluster.incNextAlloc(node.id, res.resType, res.value)
				cluster.updateNextGama(node.id, res.resType)
			}
			// 预分配当前节点与下游服务节点的带宽
			for _, dep := range m.app[aid].dep[ms.id] {
				um := m.app[aid].ms[dep.dmId]
				cluster.incNextBandAlloc(node.id, um.placeNode, dep.trans)
			}
		}
		return ms2node, nil
	}

	// partition
	c0, c1 := m.nodePartition(cluster)                           // 在最小分割数的基础上选出最小链路通信开销的工作节点划分方案
	mss0, mss1, err := m.microservicePartition(aid, mss, c0, c1) // 根据通信开销、网络干扰和资源碎片将微服务划分到 c0 或 c1 分区
	if err != nil {
		return ms2node, err
	}
	ms2node0, err := m.recursiveMapping(aid, mss0, c0) // 递归处理 c0 分区
	if err != nil {
		return ms2node, err
	}
	ms2node1, err := m.recursiveMapping(aid, mss1, c1) // 递归处理 c1 分区
	if err != nil {
		return ms2node, err
	}
	for mid, nid := range ms2node0 { // ms2node = ms2node0 + ms2node1
		ms2node[mid] = nid
	}
	for mid, nid := range ms2node1 {
		ms2node[mid] = nid
	}
	return ms2node, nil
}

func (m *MOTAS) doPlacement(ms2node map[msId]nodeId) {
	//TODO：
	// - 1. 与 k8s 进行交互
	// - 2. 调用相关 commit 操作更新集群资源状态
}

// nodePartition 使用 Fiduccia-Mattheyses 算法得到具有最小分割（cut size）的工作节点划分方案
func (m *MOTAS) nodePartition(cluster *Cluster) (*Cluster, *Cluster) {
	// FM 算法对图进行分割
	records := cluster.hyperGraphPartition()
	// 在这些方案中搜索左右分区具有最小链路通信成本（cost）的方案
	var (
		minCost float32 = math.MaxFloat32 / 2
		cost    float32 = 0
		minIdx          = 0
	)

	for idx, record := range records {
		cost = 0
		for _, lid := range record.left { // 假设节点通信是无向的
			for _, rid := range record.right {
				if link, ok := cluster.links[lid][rid]; ok {
					cost += link.cost
				}
			}
		}
		if cost < minCost {
			minCost = cost
			minIdx = idx
		}
	}

	c0 := &Cluster{
		nodes: make(map[nodeId]*Node),
		links: cluster.links,
		hpg:   nil,
	}
	c1 := &Cluster{
		nodes: make(map[nodeId]*Node),
		links: cluster.links,
		hpg:   nil,
	}
	for _, nid := range records[minIdx].left {
		c0.nodes[nid] = cluster.nodes[nid]
	}
	for _, nid := range records[minIdx].right {
		c1.nodes[nid] = cluster.nodes[nid]
	}

	return c0, c1
}

func (m *MOTAS) microservicePartition(aid appId, mss map[msId]*Microservice, cluster0 *Cluster, cluster1 *Cluster) (
	map[msId]*Microservice, map[msId]*Microservice, error) {

	ms0 := make(map[msId]*Microservice)
	ms1 := make(map[msId]*Microservice)
	// 按照拓扑序对微服务进行遍历（若 A 调用 B，则 A 依赖 B，拓扑序为：B、A）
	order := m.app[aid].getTopologyOrder()
	for _, mid := range order {
		if _, ok := mss[mid]; !ok {
			continue
		}

		// 过滤掉不满足资源需求或违背资源平衡性的工作节点，err 只有资源不足一种错误类型
		node0, err := cluster0.filterBalanceNode(m.app[aid], mid)
		if err != nil {
			return ms0, ms1, err
		}
		node1, err := cluster1.filterBalanceNode(m.app[aid], mid)
		if err != nil {
			return ms0, ms1, err
		}
		// 分别计算该微服务在两个分区中的最小通信成本
		cost0, n0, path0 := m.getMinCost(aid, mid, node0)
		cost1, n1, path1 := m.getMinCost(aid, mid, node1)
		// 分别计算该微服务在两个分区中最小通信成本节点上的网络干扰
		inter0 := m.getInter(aid, mid, n0, path0)
		inter1 := m.getInter(aid, mid, n1, path1)
		// 分别计算该微服务在两个分区中最小通信成本节点上的资源碎片情况
		frag0 := m.getFrag(aid, mid, n0)
		frag1 := m.getFrag(aid, mid, n1)
		// 分别计算该微服务在两个分区上的效用值
		score0 := m.score(cost0, inter0, frag0)
		score1 := m.score(cost1, inter1, frag1)
		if score0 < score1 {
			ms0[mid] = mss[mid]
		} else {
			ms1[mid] = mss[mid]
		}
	}

	return ms0, ms1, nil
}

func (m *MOTAS) getMinCost(aid appId, mid msId, srcs []nodeId) (float32, nodeId, map[nodeId][]nodeId) {
	app := m.app[aid]
	dests := make([]nodeId, 0, len(app.dep[mid]))
	for _, dep := range app.dep[mid] {
		if nid := app.ms[dep.dmId].placeNode; nid != NotPlaced {
			dests = append(dests, nid)
		}
	}

	var minSrc nodeId
	var minCost float32 = math.MaxFloat32
	var minCostPaths map[nodeId][]nodeId // dest node -> path of from src to dest
	for _, src := range srcs {
		cost, paths := m.cluster.minimalCostPath(src, dests)
		if cost < minCost {
			minSrc = src
			minCost = cost
			minCostPaths = paths
		}
	}

	return minCost, minSrc, minCostPaths
}

func (m *MOTAS) getInter(aid appId, mid msId, nid nodeId, path map[nodeId][]nodeId) float32 {
	var inter float32 = 0
	links := m.cluster.links
	app := m.app[aid]
	for _, dep := range app.dep[mid] { // ms of mid -- call --> ms of dep.dmId
		var from nodeId
		for i, to := range path[app.ms[dep.dmId].placeNode] {
			if i == 0 { // first node
				from = to
				continue
			}
			link := links[from][to]
			inter += dep.trans / (link.bandCap - link.nextBandAlloc)
			from = to
		}
	}

	return inter
}

func (m *MOTAS) getFrag(aid appId, mid msId, nid nodeId) float32 {
	var (
		ms           = m.app[aid].ms[mid]
		frag float32 = 0 // final ret
		gama float32
		r    float32
		f    float32
	)

	for _, node := range m.cluster.nodes {
		r = 0
		for _, typ := range node.resType {
			if node.id == nid {
				gama = (node.nextAlloc[typ].value + ms.resReq[typ].value) / node.capa[typ].value
			} else {
				gama = node.nextAlloc[typ].value / node.capa[typ].value
			}
			r += node.args[typ] * gama
		}
		r /= float32(len(node.resType))

		f = 0
		for _, typ := range node.resType {
			if node.id == nid {
				gama = (node.nextAlloc[typ].value + ms.resReq[typ].value) / node.capa[typ].value
			} else {
				gama = node.nextAlloc[typ].value / node.capa[typ].value
			}

			f += (gama - r) * (gama - r)
		}
		f /= float32(len(node.resType))
		f = float32(math.Sqrt(float64(f)))
		frag += f
	}

	return frag
}

func (m *MOTAS) score(cost, inter, frag float32) float32 {
	return m.alphaC*cost + m.alphaI*inter + m.alphaF*frag
}
