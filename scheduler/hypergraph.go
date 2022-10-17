package scheduler

import (
	"fmt"
	"math"
)

//
// HyperGraph 超图，使用 FM 算法对集群节点进行分区
//
type HyperGraph struct {
	cells        map[nodeId]*Cell
	indexes      map[nodeId][]int // node id -> hyper edge index
	hyperEdge    map[int][]nodeId // index -> node id list
	left, right  *Partition       // divide the cluster into two partition
	minRecordIdx []int            // indexes of the partition records with the min cut size
	Records      []*Record
	minCutSize   int
}

func newHyperGraph(c *Cluster) *HyperGraph {
	hpg := &HyperGraph{
		cells:        make(map[nodeId]*Cell),
		indexes:      make(map[nodeId][]int),
		hyperEdge:    make(map[int][]nodeId),
		minRecordIdx: make([]int, 0),
		Records:      make([]*Record, 0),
	}
	for _, node := range c.nodes {
		hpg.cells[node.id] = &Cell{id: node.id}
	}
	idx := 0

	// 初始化超图中的超边
	for fromId, toLinks := range c.links {
		if len(toLinks) == 0 {
			continue
		}

		hpg.indexes[fromId] = append(hpg.indexes[fromId], idx)
		hpg.hyperEdge[idx] = append(hpg.hyperEdge[idx], fromId)
		for toId := range toLinks {
			hpg.indexes[toId] = append(hpg.indexes[toId], idx)
			hpg.hyperEdge[idx] = append(hpg.hyperEdge[idx], toId)
		}
		idx++
	}

	hpg.left, hpg.right = hpg.randomPartition() // 随机分区
	fmt.Println("hyperGraph", hpg.left)
	fmt.Println("hyperGraph", hpg.right)
	hpg.minCutSize = hpg.computeCutSize() // 计算初始化状态的 cut size
	hpg.initGains()                       // 初始化 gain 值
	hpg.Records = append(hpg.Records, newRecord("-", 0, 0, hpg.minCutSize, hpg.left.cellsFormat(), hpg.right.cellsFormat()))
	hpg.minRecordIdx = append(hpg.minRecordIdx, 0)
	return hpg
}

func (h *HyperGraph) fmRun() {
	for h.left.remain != 0 || h.right.remain != 0 {
		// 选择具有最大 gain 值的 cell，并将其换至另一个分区
		swapped, affected := h.selectAndSwap()

		// 重新计算受影响的 cell 的 gain 值
		h.computeGains(affected)

		// 构造并存储分割记录，另外还需记录最小 cut size 记录对应的下标
		record := h.structureRecord(swapped)
		h.Records = append(h.Records, record)
		if record.cutSize < h.minCutSize && record.cutSize > 0 {
			h.minCutSize = record.cutSize
			h.minRecordIdx = make([]int, 0)
			h.minRecordIdx = append(h.minRecordIdx, len(h.Records)-1)
		} else if record.cutSize == h.minCutSize {
			h.minRecordIdx = append(h.minRecordIdx, len(h.Records)-1)
		}
	}
}

func (h *HyperGraph) minCutSizeRecords() []*Record {
	ret := make([]*Record, 0)
	for _, idx := range h.minRecordIdx {
		ret = append(ret, h.Records[idx])
	}
	return ret
}

// Cell 顶点结构，一个顶点对应着集群中的一个工作节点
type Cell struct {
	id        nodeId
	gain      int
	isSwapped bool
	partition int
}

const (
	LeftPart int = iota
	RightPart
)

// Partition 分区结构
type Partition struct {
	maxGainId nodeId
	maxGain   int
	cellIds   map[nodeId]struct{}
	remain    int
}

func (p *Partition) cellsFormat() []nodeId {
	ret := make([]nodeId, 0, len(p.cellIds))
	for id := range p.cellIds {
		ret = append(ret, id)
	}
	return ret
}

// Record 分割记录
type Record struct {
	cell    nodeId
	gain    int
	sumGain int
	cutSize int
	left    []nodeId
	right   []nodeId
}

func newRecord(cell nodeId, gain, sumGain, cutSize int, left, right []nodeId) *Record {
	return &Record{
		cell:    cell,
		gain:    gain,
		sumGain: sumGain,
		cutSize: cutSize,
		left:    left,
		right:   right,
	}
}

func (h *HyperGraph) randomPartition() (*Partition, *Partition) {
	left := &Partition{
		maxGain: math.MinInt,
		cellIds: make(map[nodeId]struct{}),
	}
	right := &Partition{
		maxGain: math.MinInt,
		cellIds: make(map[nodeId]struct{}),
	}
	cnt := 0
	for id := range h.cells {
		if cnt%2 == 0 {
			h.cells[id].partition = LeftPart
			left.cellIds[id] = struct{}{}
			left.remain++
		} else {
			h.cells[id].partition = RightPart
			right.cellIds[id] = struct{}{}
			right.remain++
		}
		cnt++
	}
	return left, right
}

func (h *HyperGraph) computeCutSize() int {
	cutSize := 0
	var partition int
	for _, edge := range h.hyperEdge {
		if len(edge) == 1 {
			continue
		}

		i := 0
		for _, nid := range edge {
			if _, ok := h.cells[nid]; !ok {
				continue
			}
			if i == 0 {
				partition = h.cells[nid].partition
			} else if partition != h.cells[nid].partition {
				cutSize++
				break
			}
			i++
		}
	}
	return cutSize
}

func (h *HyperGraph) initGains() {
	for _, curCell := range h.cells {
		fs, te := 0, 0
		for _, edge := range h.getEdgeById(curCell.id) {
			samePar := 1
			for _, nid := range edge {
				if _, ok := h.cells[nid]; !ok {
					continue
				}
				if curCell.id == nid {
					continue
				}
				if curCell.partition == h.cells[nid].partition {
					samePar++
				}
			}
			if samePar == 1 {
				fs++
			}
			if samePar == len(edge) {
				te++
			}
		}
		curCell.gain = fs + te
		if curCell.partition == LeftPart && curCell.gain > h.left.maxGain {
			h.left.maxGain = curCell.gain
			h.left.maxGainId = curCell.id
		}
		if curCell.partition == RightPart && curCell.gain > h.right.maxGain {
			h.right.maxGain = curCell.gain
			h.right.maxGainId = curCell.id
		}
	}
}

func (h *HyperGraph) computeGains(affected []nodeId) {
	for _, curId := range affected { // only gains are calculated for cells affected by the swap op
		fs, te := 0, 0
		for _, edge := range h.getEdgeById(curId) {
			samePar := 1
			for _, nid := range edge {
				if _, ok := h.cells[nid]; !ok {
					continue
				}
				if curId == nid {
					continue
				}
				if h.cells[curId].partition == h.cells[nid].partition {
					samePar++
				}
			}
			if samePar == 1 {
				fs++
			}
			if samePar == len(edge) {
				te++
			}
		}
		h.cells[curId].gain = fs - te // not the same as `initGains`
		if h.cells[curId].partition == LeftPart && h.cells[curId].gain > h.left.maxGain {
			h.left.maxGain = h.cells[curId].gain
			h.left.maxGainId = h.cells[curId].id
		}
		if h.cells[curId].partition == RightPart && h.cells[curId].gain > h.right.maxGain {
			h.right.maxGain = h.cells[curId].gain
			h.right.maxGainId = h.cells[curId].id
		}
	}
}

func (h *HyperGraph) getEdgeById(nid nodeId) map[int][]nodeId {
	ret := make(map[int][]nodeId)
	for _, idx := range h.indexes[nid] {
		ret[idx] = h.hyperEdge[idx]
	}
	return ret
}

func (h *HyperGraph) selectAndSwap() (nodeId, []nodeId) {
	// swap 操作
	var swap nodeId
	if h.right.remain >= h.left.remain { // right -> left
		swap = h.right.maxGainId
		h.left.cellIds[swap] = struct{}{}
		delete(h.right.cellIds, swap)
		h.right.remain--
	} else { // left -> right
		swap = h.left.maxGainId
		h.right.cellIds[swap] = struct{}{}
		delete(h.left.cellIds, swap)
		h.left.remain--
	}
	h.cells[swap].partition ^= 1
	h.cells[swap].isSwapped = true

	// 找到被 swap 影响的 cells，也就是和 swap 在同条超边上的点
	retAffected := make([]nodeId, 0)
	affected := make(map[nodeId]struct{})
	for _, edge := range h.getEdgeById(swap) {
		for _, afId := range edge {
			if _, ok := h.cells[afId]; !ok {
				continue
			}
			if afId != swap && !h.cells[afId].isSwapped {
				retAffected = append(retAffected, afId)
				affected[afId] = struct{}{}
			}
		}
	}

	// 重新计算各分区中的 max gain，不考虑受影响的 cells，因为之后会重新计算它们的 gain 值
	h.left.maxGain = math.MinInt
	for id := range h.left.cellIds {
		if _, ok := affected[id]; ok {
			continue
		}
		if !h.cells[id].isSwapped && h.cells[id].gain > h.left.maxGain {
			h.left.maxGain = h.cells[id].gain
			h.left.maxGainId = id
		}
	}
	h.right.maxGain = math.MinInt
	for id, _ := range h.right.cellIds {
		if _, ok := affected[id]; ok {
			continue
		}
		if !h.cells[id].isSwapped && h.cells[id].gain > h.right.maxGain {
			h.right.maxGain = h.cells[id].gain
			h.right.maxGainId = id
		}
	}

	return swap, retAffected
}

func (h *HyperGraph) structureRecord(swap nodeId) *Record {
	return &Record{
		cell:    swap,
		gain:    h.cells[swap].gain,
		sumGain: h.cells[swap].gain + h.Records[len(h.Records)-1].sumGain,
		cutSize: h.computeCutSize(),
		left:    h.left.cellsFormat(),
		right:   h.right.cellsFormat(),
	}
}
