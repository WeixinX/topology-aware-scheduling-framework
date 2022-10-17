package scheduler

import (
	"errors"
	"math"
	
	"github.com/jinzhu/copier"

	"github.com/WeixinX/topology-aware-scheduling-framework/util"
)

//
// Service Profile
//
type Service struct {
	id            appId
	rootId        msId // eg. A -- call --> B -- call --> C, B -- call --> D, A is root
	ms            map[msId]*Microservice
	dep           map[msId][]*Dependence // um -> dm list
	reDep         map[msId][]*Dependence // dm -> um list
	levelOrder    []msId                 // order of level travel
	topologyOrder []msId                 // order of topology from callee to caller
	priority      int
}

type Microservice struct {
	id            msId
	resReq        map[ResourceType]Resource
	placeNode     nodeId
	nextPlaceNode nodeId
}

type Dependence struct {
	umId  msId
	dmId  msId
	trans float32
}

func (s *Service) msCount() int {
	return len(s.ms)
}

func (s *Service) decPriority() {
	s.priority--
}

func (s *Service) getTopologyOrder() []msId {
	if s.topologyOrder == nil || len(s.topologyOrder) != s.msCount() {
		s.topologyOrder = s.topologyTravel()
	}
	return s.topologyOrder
}

// topologyTravel 获取微服务反向拓扑序，当 A -- call --> B，in-degree(A) + 1
func (s *Service) topologyTravel() []msId {
	order := make([]msId, 0, s.msCount())
	inDegree := make(map[msId]int) // msId -> in-degree
	for _, deps := range s.reDep {
		for _, dep := range deps {
			if _, ok := inDegree[dep.umId]; !ok {
				inDegree[dep.umId] = 0
			}
			if _, ok := inDegree[dep.dmId]; !ok {
				inDegree[dep.dmId] = 0
			}
			inDegree[dep.umId]++
		}
	}
	q := util.NewQueue(s.msCount())
	for id, in := range inDegree {
		if in == 0 {
			q.Push(id)
		}
	}
	for !q.Empty() {
		mid := q.Pop().(msId)
		order = append(order, mid)
		for _, dep := range s.reDep[mid] {
			if _, ok := inDegree[dep.umId]; !ok {
				continue
			}
			inDegree[dep.umId]--
			if inDegree[dep.umId] == 0 {
				q.Push(dep.umId)
			}
		}
	}
	return order
}

func (s *Service) getLevelOrder() []msId {
	if s.levelOrder == nil || len(s.levelOrder) != s.msCount() {
		s.levelOrder = s.levelTravel()
	}
	return s.levelOrder
}

// levelTravel 按照拓扑关系层次遍历
func (s *Service) levelTravel() []msId {
	order := make([]msId, 0, s.msCount())
	q := util.NewQueue(s.msCount())
	q.Push(s.rootId)

	for !q.Empty() {
		mid := q.Pop().(msId)
		order = append(order, mid)
		for _, dep := range s.dep[mid] {
			q.Push(dep.dmId)
		}
	}
	return order
}

// setNextPlaceNode 预放置
func (s *Service) setNextPlaceNode(mid msId, nid nodeId) {
	s.ms[mid].nextPlaceNode = nid
}

// rollbackPlaceStat 回滚微服务放置节点位置
func (s *Service) rollbackPlaceStat() {
	for _, ms := range s.ms {
		s.ms[ms.id].nextPlaceNode = ms.placeNode
	}
}

// commitPlaceStat 确认微服务放置节点位置
func (s *Service) commitPlaceStat() {
	for _, ms := range s.ms {
		s.ms[ms.id].placeNode = ms.nextPlaceNode
	}
}

//
// Cluster Profile
//
type Cluster struct {
	nodes map[nodeId]*Node            // node id -> node
	links map[nodeId]map[nodeId]*Link // node id A, B -> link_{A, B}
	hpg   *HyperGraph                 // hyper graph is used on the fm algorithm
}

type Node struct {
	id          nodeId
	resType     []ResourceType
	capa        map[ResourceType]*Resource // capacity
	alloc       map[ResourceType]*Resource // allocated
	nextAlloc   map[ResourceType]*Resource // assumptive allocated // FIXME 在确定一个服务的放置位置后（或调度失败）修改（或回溯）该值，而 alloc 在真正部署后再修改
	args        map[ResourceType]float32   // arguments
	maxGama     float32
	minGama     float32
	nextMaxGama float32
	nextMinGama float32
	threshold   float32 // this is T in paper
}

type Link struct {
	// endpoint from -> endpoint to
	from          nodeId
	to            nodeId
	cost          float32
	bandCap       float32
	bandAlloc     float32
	nextBandAlloc float32
}

type Resource struct {
	resType ResourceType
	value   float32
}

type ResourceType uint

func (c *Cluster) nodeCount() int {
	return len(c.nodes)
}

// filterBalanceNode
func (c *Cluster) filterBalanceNode(app *Service, mid msId) ([]nodeId, error) {
	// condition 1: resource capacity
	n1 := make([]nodeId, 0)
	for _, node := range c.nodes {
		cond1 := true
		for typ, req := range app.ms[mid].resReq {
			capa := node.capa[typ].value
			alloc := node.nextAlloc[typ].value
			if req.value+alloc > capa {
				DLogINFO("cond1: (ms=%s, type=%v, req=%.2f), (node=%s, alloc/cap=%.2f/%.2f)",
					mid, typ, req.value, node.id, node.nextAlloc[typ].value, node.capa[typ].value)
				cond1 = false
				break
			}
		}
		if cond1 {
			n1 = append(n1, node.id)
		}
	}
	if len(n1) == 0 {
		return n1, errors.New("out of resources") // 资源不足
	}
	DLogINFO("satisfy cond 1: %v", n1)

	// condition 2: resource fragment
	n2 := make([]nodeId, 0)
	for _, nid := range n1 {
		maxGama, minGama := c.nodes[nid].nextMaxGama, c.nodes[nid].nextMinGama
		for typ, req := range app.ms[mid].resReq {
			capa := c.nodes[nid].capa[typ].value
			alloc := c.nodes[nid].nextAlloc[typ].value
			gama := (alloc + req.value) / capa
			if gama < minGama {
				minGama = gama
			}
			if gama > maxGama {
				maxGama = gama
			}
		}
		if maxGama-minGama > c.nodes[nid].threshold {
			DLogINFO("cond2: max-min(%.2f-%.2f=%.2f) beyond threshold(=%.2f), (ms=%s), (node=%s)",
				maxGama, minGama, maxGama-minGama, c.nodes[nid].threshold, mid, nid)
			continue
		}
		n2 = append(n2, nid)
	}
	DLogINFO("satisfy cond 2: %v", n2)

	// condition 3: bandwidth available of the link
	// FIXME 两端点不一定直达，可能中间有很多条 link，那么需要考虑每一条 link 的可用带宽，那这可能还需要考虑交换机...先假设可直达吧
	canPlaceN := make([]nodeId, 0)
	for _, nid := range n2 {
		cond3 := true
		for _, dep := range app.dep[mid] {
			dest := app.ms[dep.dmId].nextPlaceNode
			if dest != NotPlaced {
				link, ok := c.links[nid][dest]
				if !ok || dep.trans+link.nextBandAlloc > link.bandCap {
					if ok {
						DLogINFO("cond3: (from=%s, to=%s, trans=%.2f), (from=%s, to=%s, band alloc/cap=%.2f/%.2f)",
							dep.umId, dep.dmId, dep.trans, nid, dest, link.nextBandAlloc, link.bandCap)
					}
					cond3 = false
					break
				}
			}
		}
		if cond3 {
			canPlaceN = append(canPlaceN, nid)
		}
	}
	DLogINFO("satisfy cond 3 for ms(id=%s): %v", mid, canPlaceN)

	return canPlaceN, nil
}

// minimalCostPath 使用 Dijkstra 算法（堆优化）计算 `src` 到达 `dest` 的最小花费和路径
func (c *Cluster) minimalCostPath(src nodeId, dests []nodeId) (float32, map[nodeId][]nodeId) {
	var maxCost float32 = math.MaxFloat32 / 2 // 假设该值足够大
	visit := make(map[nodeId]bool)
	cost := make(map[nodeId]float32)
	path := make(map[nodeId]nodeId)
	for nid, _ := range c.nodes {
		cost[nid] = maxCost
		visit[nid] = false
	}
	cost[src] = 0
	path[src] = PrevNull

	q := newDjQueue(len(c.nodes))
	q.push(src, 0)
	for !q.empty() {
		mid := q.pop()
		visit[mid] = true
		for next, link := range c.links[mid] {
			if !visit[next] && cost[next] > cost[mid]+link.cost {
				cost[next] = cost[mid] + link.cost
				path[next] = mid
				q.push(next, cost[next])
			}
		}
	}

	var retCost float32 = 0
	for _, nid := range dests {
		if ct, ok := cost[nid]; ok && ct != maxCost {
			retCost += ct
		}
	}

	return retCost, structurePaths(path, dests)
}

func structurePaths(row map[nodeId]nodeId, dests []nodeId) map[nodeId][]nodeId {
	s := newPathStack()
	ret := make(map[nodeId][]nodeId)
	for _, nid := range dests {
		if _, ok := row[nid]; !ok {
			continue
		}
		s.push(nid)
		i := nid
		for row[i] != PrevNull {
			s.push(row[i])
			i = row[i]
		}
		if _, ok := ret[nid]; !ok {
			ret[nid] = make([]nodeId, 0, s.size())
		}
		for !s.empty() {
			id := s.pop()
			ret[nid] = append(ret[nid], id)
		}
	}

	return ret
}

// hyperGraphPartition 初始化并运行超图，最终返回拥有最小 cut size 的分割结果
func (c *Cluster) hyperGraphPartition() []*Record {
	c.buildHyperGraph()
	c.runHyperGraph()
	return c.getMinCutSizeRecords()
}

// buildHyperGraph 初始化超图
func (c *Cluster) buildHyperGraph() {
	c.hpg = newHyperGraph(c)
}

// runHyperGraph 运行超图得到分割结果
func (c *Cluster) runHyperGraph() {
	c.hpg.fmRun()
}

// getMinCutSizeRecords 返回拥有最小 cut size 的分割结果
func (c *Cluster) getMinCutSizeRecords() []*Record {
	return c.hpg.minCutSizeRecords()
}

// incAllNextAlloc 预分配所有资源
func (c *Cluster) incAllNextAlloc(nid nodeId, req map[ResourceType]Resource) {
	for typ, res := range req {
		c.incNextAlloc(nid, typ, res.value)
	}
}

// incNextAlloc 预分配资源
func (c *Cluster) incNextAlloc(nid nodeId, typ ResourceType, inc float32) {
	c.nodes[nid].nextAlloc[typ].value += inc
}

// decAllNextAlloc 回收所有预分配资源
func (c *Cluster) decAllNextAlloc(nid nodeId, req map[ResourceType]Resource) {
	for typ, res := range req {
		c.decNextAlloc(nid, typ, res.value)
	}
}

// decNextAlloc 回收预分配资源
func (c *Cluster) decNextAlloc(nid nodeId, typ ResourceType, inc float32) {
	c.nodes[nid].nextAlloc[typ].value -= inc
}

// rollbackPartitionNextAlloc 回滚一个分区预分配资源
func (c *Cluster) rollbackPartitionNextAlloc(nodes map[nodeId]*Node) {
	for _, node := range nodes {
		for _, typ := range node.resType {
			node.nextAlloc[typ].value = node.alloc[typ].value
		}
	}
}

// rollbackAlloc 将预分配资源回滚到原来的状态
func (c *Cluster) rollbackAlloc() {
	for _, node := range c.nodes {
		for _, typ := range node.resType {
			node.nextAlloc[typ].value = node.alloc[typ].value
		}
	}
}

// commitAlloc 确认预分配资源提交
func (c *Cluster) commitAlloc() {
	for _, node := range c.nodes {
		for _, typ := range node.resType {
			node.alloc[typ].value = node.nextAlloc[typ].value
		}
	}
}

// updateNextGama 更新资源利用率情况 gama
func (c *Cluster) updateNextGama(nid nodeId) {
	var minGama float32 = math.MaxFloat32 / 2
	var maxGama float32 = 0
	node := c.nodes[nid]

	for _, typ := range node.resType {
		alloc := node.nextAlloc[typ].value
		capa := node.capa[typ].value
		gama := alloc / capa
		//fmt.Println(alloc, capa)
		if gama > maxGama {
			maxGama = gama
		}
		if gama < minGama {
			minGama = gama
		}
	}
	node.nextMinGama = minGama
	node.nextMaxGama = maxGama
}

// rollbackGama 回滚资源利用率情况
func (c *Cluster) rollbackGama() {
	for _, node := range c.nodes {
		node.nextMaxGama = node.maxGama
		node.nextMinGama = node.minGama
	}
}

// commitGama 确认资源利用率
func (c *Cluster) commitGama() {
	for _, node := range c.nodes {
		node.maxGama = node.nextMaxGama
		node.minGama = node.nextMinGama
	}
}

// incNextBandAlloc 预分配链路带宽
func (c *Cluster) incNextBandAlloc(from, to nodeId, inc float32) {
	// 假设无向
	if link, ok := c.links[from][to]; ok {
		link.nextBandAlloc += inc
	}
	if from != to {
		if link, ok := c.links[to][from]; ok {
			link.nextBandAlloc += inc
		}
	}
}

// decNextBandAlloc 回收预分配链路带宽
func (c *Cluster) decNextBandAlloc(from, to nodeId, inc float32) {
	// 假设无向
	if link, ok := c.links[from][to]; ok {
		link.nextBandAlloc -= inc
	}
	if from != to {
		if link, ok := c.links[to][from]; ok {
			link.nextBandAlloc -= inc
		}
	}
}

func (c *Cluster) rollbackBandAlloc() {
	for _, toList := range c.links {
		for _, link := range toList {
			link.nextBandAlloc = link.bandAlloc
		}
	}
}

func (c *Cluster) commitBandAlloc() {
	for _, toList := range c.links {
		for _, link := range toList {
			link.bandAlloc = link.nextBandAlloc
		}
	}
}

// rollbackStat 回滚集群状态
func (c *Cluster) rollbackStat() {
	c.rollbackAlloc()
	c.rollbackGama()
	c.rollbackBandAlloc()
}

// clone 克隆集群状态，可用于撤销
func (c *Cluster) clone() *Cluster {
	// 对于结构体中有 map 类型且未导出的字段，需要对其进行初始化然后再深拷贝
	ret := Cluster{
		nodes: map[nodeId]*Node{},
		links: map[nodeId]map[nodeId]*Link{},
		hpg:   nil,
	}
	copier.CopyWithOption(&ret.nodes, c.nodes, copier.Option{DeepCopy: true})
	copier.CopyWithOption(&ret.links, c.links, copier.Option{DeepCopy: true}) //
	for nid, node := range c.nodes {
		ret.nodes[nid].alloc = map[ResourceType]*Resource{}
		ret.nodes[nid].capa = map[ResourceType]*Resource{}
		ret.nodes[nid].nextAlloc = map[ResourceType]*Resource{}
		ret.nodes[nid].args = map[ResourceType]float32{}
		copier.CopyWithOption(&ret.nodes[nid].alloc, &node.alloc, copier.Option{DeepCopy: true})
		copier.CopyWithOption(&ret.nodes[nid].capa, &node.capa, copier.Option{DeepCopy: true})
		copier.CopyWithOption(&ret.nodes[nid].nextAlloc, &node.nextAlloc, copier.Option{DeepCopy: true})
		copier.CopyWithOption(&ret.nodes[nid].args, &node.args, copier.Option{DeepCopy: true})
	}
	//for from, links := range c.links {
	//	ret.links[from] = map[nodeId]*Link{}
	//	for to, link := range links {
	//		v := *link
	//		ret.links[from][to] = &v
	//	}
	//}
	return &ret
}
