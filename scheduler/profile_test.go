package scheduler

import (
	"fmt"
	"math"
	"testing"
)

var (
	tResType = []ResourceType{ResCPU, ResMem}
	tResReq  = map[ResourceType]Resource{
		ResCPU: {ResCPU, 2},       // cores
		ResMem: {ResMem, 60 * MB}, // MB
	}
	tService = &Service{
		id:     "test0",
		rootId: "A",
		ms: map[msId]*Microservice{
			"A": {
				id:            "A",
				resReq:        tResReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"B": {
				id:            "B",
				resReq:        tResReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"C": {
				id:            "C",
				resReq:        tResReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"D": {
				id:            "D",
				resReq:        tResReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"E": {
				id:            "E",
				resReq:        tResReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"F": {
				id:            "F",
				resReq:        tResReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
		},
		dep: map[msId][]*Dependence{
			"A": {
				{"A", "B", 1 * MB},
				{"A", "C", 1 * MB},
			},
			"B": {
				{"B", "D", 1 * MB},
				{"B", "E", 1 * MB},
			},
			"C": {
				{"C", "D", 1 * MB},
				{"C", "F", 1 * MB},
			},
		},
		reDep: map[msId][]*Dependence{
			"B": {
				{"A", "B", 1 * MB},
			},
			"C": {
				{"A", "C", 1 * MB},
			},
			"D": {
				{"B", "D", 1 * MB},
				{"C", "D", 1 * MB},
			},
			"E": {
				{"B", "E", 1 * MB},
			},
			"F": {
				{"C", "F", 1 * MB},
			},
		},
		priority: 5,
	}
	tCluster = &Cluster{
		nodes: map[nodeId]*Node{
			"node0": {
				id:          "node0",
				resType:     tResType,
				capa:        map[ResourceType]*Resource{ResCPU: {ResCPU, 8}, ResMem: {ResMem, 32 * GB}},
				alloc:       map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				nextAlloc:   map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				args:        map[ResourceType]float32{ResCPU: 0.5, ResMem: 0.5},
				minGama:     math.MaxFloat32,
				nextMinGama: math.MaxFloat32,
				threshold:   0.8,
			},
			"node1": {
				id:          "node1",
				resType:     tResType,
				capa:        map[ResourceType]*Resource{ResCPU: {ResCPU, 8}, ResMem: {ResMem, 32 * GB}},
				alloc:       map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				nextAlloc:   map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				args:        map[ResourceType]float32{ResCPU: 0.5, ResMem: 0.5},
				minGama:     math.MaxFloat32,
				nextMinGama: math.MaxFloat32,
				threshold:   0.8,
			},
			"node2": {
				id:          "node2",
				resType:     tResType,
				capa:        map[ResourceType]*Resource{ResCPU: {ResCPU, 8}, ResMem: {ResMem, 32 * GB}},
				alloc:       map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				nextAlloc:   map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				args:        map[ResourceType]float32{ResCPU: 0.5, ResMem: 0.5},
				minGama:     math.MaxFloat32,
				nextMinGama: math.MaxFloat32,
				threshold:   0.8,
			},
			"node3": {
				id:          "node3",
				resType:     tResType,
				capa:        map[ResourceType]*Resource{ResCPU: {ResCPU, 8}, ResMem: {ResMem, 32 * GB}},
				alloc:       map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				nextAlloc:   map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				args:        map[ResourceType]float32{ResCPU: 0.5, ResMem: 0.5},
				minGama:     math.MaxFloat32,
				nextMinGama: math.MaxFloat32,
				threshold:   0.8,
			},
		},
		links: map[nodeId]map[nodeId]*Link{
			"node0": {
				"node1": &Link{
					from:    "node0",
					to:      "node1",
					cost:    1,
					bandCap: 30 * MB,
				},
				"node2": &Link{
					from:    "node0",
					to:      "node2",
					cost:    1,
					bandCap: 30 * MB,
				},
				"node3": &Link{
					from:    "node0",
					to:      "node3",
					cost:    1,
					bandCap: 30 * MB,
				},
			},
			"node1": {
				"node0": &Link{
					from:    "node1",
					to:      "node0",
					cost:    1,
					bandCap: 30 * MB,
				},
				"node2": &Link{
					from:    "node1",
					to:      "node2",
					cost:    1,
					bandCap: 30 * MB,
				},
				"node3": &Link{
					from:    "node1",
					to:      "node3",
					cost:    1,
					bandCap: 30 * MB,
				},
			},
			"node2": {
				"node0": &Link{
					from:    "node2",
					to:      "node0",
					cost:    1,
					bandCap: 30 * MB,
				},
				"node1": &Link{
					from:    "node2",
					to:      "node1",
					cost:    1,
					bandCap: 30 * MB,
				},
				"node3": &Link{
					from:    "node2",
					to:      "node3",
					cost:    1,
					bandCap: 30 * MB,
				},
			},
			"node3": {
				"node0": &Link{
					from:    "node3",
					to:      "node0",
					cost:    1,
					bandCap: 30 * MB,
				},
				"node1": &Link{
					from:    "node3",
					to:      "node1",
					cost:    1,
					bandCap: 30 * MB,
				},
				"node2": &Link{
					from:    "node3",
					to:      "node2",
					cost:    1,
					bandCap: 30 * MB,
				},
			},
		},
	}
)

func TestServiceProfile(t *testing.T) {
	fmt.Println("ms count: ", tService.msCount())
	fmt.Println("priority: ", tService.priority)
	tService.decPriority()
	fmt.Println("decrease priority: ", tService.priority)

	order := tService.getTopologyOrder()
	fmt.Printf("topology order: ")
	for _, id := range order {
		fmt.Printf("%s ", id)
	}
	fmt.Println()

	tService.setNextPlaceNode("A", "node0")
	fmt.Printf("next placement: service %s -> cluster %s\n", tService.ms["A"].id, tService.ms["A"].nextPlaceNode)
	tService.commitPlaceStat()
	fmt.Printf("commit placement: service %s -> cluster %s\n", tService.ms["A"].id, tService.ms["A"].placeNode)
}

func TestClusterProfile(t *testing.T) {
	fmt.Println("node count: ", tCluster.nodeCount())

	minCost, path := tCluster.minimalCostPath("node0", []nodeId{"node1", "node2", "node3"})
	fmt.Println("path from node0 to other node: ")
	for id, ids := range path {
		fmt.Printf("node0 -> %s: ", id)
		for _, n := range ids {
			fmt.Printf("%s ", n)
		}
		fmt.Println()
	}
	fmt.Println("total min cost: ", minCost)

	tCluster.incNextAlloc("node0", ResCPU, 4)
	tCluster.updateNextGama("node0", ResCPU)
	tCluster.incNextAlloc("node0", ResMem, 30*GB)
	tCluster.updateNextGama("node0", ResMem)
	tCluster.incNextBandAlloc("node0", "node1", KB)
	fmt.Println("next alloc cpu: ", tCluster.nodes["node0"].nextAlloc[ResCPU].value)
	fmt.Println("next band alloc from 0 to 1: ", tCluster.links["node0"]["node1"].nextBandAlloc)
	fmt.Println("next band alloc from 1 to 0: ", tCluster.links["node1"]["node0"].nextBandAlloc)
	fmt.Println("next max gama: ", tCluster.nodes["node0"].nextMaxGama)
	fmt.Println("next min gama: ", tCluster.nodes["node0"].nextMinGama)
	tCluster.commitAlloc()
	fmt.Println("alloc cpu: ", tCluster.nodes["node0"].alloc[ResCPU].value)
	tCluster.commitBandAlloc()
	fmt.Println("band alloc from 0 to 1: ", tCluster.links["node0"]["node1"].bandAlloc)
	fmt.Println("band alloc from 1 to 0: ", tCluster.links["node1"]["node0"].bandAlloc)
	tCluster.commitGama()
	fmt.Println("max gama: ", tCluster.nodes["node0"].maxGama)
	fmt.Println("min gama: ", tCluster.nodes["node0"].minGama)
	fmt.Println()

	fmt.Println("hyper edges:")
	records := tCluster.hyperGraphPartition()
	for _, ids := range tCluster.hpg.hyperEdge {
		fmt.Println(ids)
	}
	fmt.Println("records: ")
	for i, record := range tCluster.hpg.Records {
		fmt.Printf("#%d: cell: %s, gain: %d, sum of gain: %d, ", i, record.cell, record.gain, record.sumGain)
		fmt.Printf("left: %v, right: %v\n", record.left, record.right)
	}

	fmt.Println("min cut size records: ")
	for i, record := range records {
		fmt.Printf("#%d: cell: %s, gain: %d, sum of gain: %d, ", i, record.cell, record.gain, record.sumGain)
		fmt.Printf("left: %v, right: %v\n", record.left, record.right)
	}
}

func TestFilterBalanceNode(t *testing.T) {
	tCluster.filterBalanceNode(tService, "A")
	fmt.Println("reduce cpu cores of node1 from 8 to 1")
	tCluster.nodes["node1"].capa[ResCPU].value = 1
	fmt.Println("reduce memory of node2 from 32G to 1M")
	tCluster.nodes["node2"].capa[ResMem].value = 1 * MB
	fmt.Println("reduce band of node0 and node3 from 30M to 0.5M")
	tService.ms["B"].placeNode = "node3"
	tCluster.links["node0"]["node3"].bandCap = 0.5 * MB
	tCluster.filterBalanceNode(tService, "A")
}
