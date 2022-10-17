package scheduler

import (
	"fmt"
	"testing"
)

func TestServiceProfile(t *testing.T) {
	tService := newTestService(DefaultResReq, DefaultBandReq)

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
	tCluster := newTestCluster(DefaultResType, DefaultResCPU, DefaultResMem, DefaultBrand)

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
	tCluster.updateNextGama("node0")
	tCluster.incNextAlloc("node0", ResMem, 100*MB)
	tCluster.updateNextGama("node0")
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
	tService := newTestService(DefaultResReq, DefaultBandReq)
	tCluster := newTestCluster(DefaultResType, DefaultResCPU, DefaultResMem, DefaultBrand)

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

func TestClusterClone(t *testing.T) {
	tCluster := newTestCluster(DefaultResType, DefaultResCPU, DefaultResMem, DefaultBrand)
	clone := tCluster.clone()
	fmt.Println(tCluster)
	fmt.Println(clone)

	fmt.Println("clone nodes: ")
	for _, node := range clone.nodes {
		fmt.Println("nid: ", node.id)
		fmt.Printf("res type: ")
		for _, typ := range node.resType {
			fmt.Printf("%v ", typ)
		}
		fmt.Println()
		fmt.Printf("capa: ")
		for typ, res := range node.capa {
			fmt.Printf("%v:%.2f ", typ, res.value)
		}
		fmt.Println()
		fmt.Printf("alloc: ")
		for typ, res := range node.alloc {
			fmt.Printf("%v:%.2f ", typ, res.value)
		}
		fmt.Println()
		fmt.Printf("next alloc: ")
		for typ, res := range node.nextAlloc {
			fmt.Printf("%v:%.2f ", typ, res.value)
		}
		fmt.Println()

		fmt.Printf("args: ")
		for typ, v := range node.args {
			fmt.Printf("%v:%.2f ", typ, v)
		}
		fmt.Println()
		fmt.Printf("max gama:%.2f, min gama:%.2f, next max gama:%.2f, next min gama:%.2f, threshold:%.2f\n", node.maxGama, node.minGama, node.nextMaxGama, node.nextMinGama, node.threshold)
	}

	fmt.Println("clone links: ")
	for from, links := range clone.links {
		for to, link := range links {
			fmt.Printf("%s->%s: next band alloc:%.2f\n", from, to, link.nextBandAlloc)
		}
	}

	for _, node := range tCluster.nodes {
		node.nextMaxGama = 100
		for _, typ := range node.resType {
			node.alloc[typ].value = 100
			node.capa[typ].value = 50
		}
	}
	for _, links := range tCluster.links {
		for _, link := range links {
			link.nextBandAlloc = 1000
		}
	}
	fmt.Printf("max gama: ")
	for _, node := range tCluster.nodes {
		fmt.Printf("%.2f ", node.nextMaxGama)
	}
	fmt.Println()
	fmt.Printf("alloc: ")
	for _, node := range tCluster.nodes {
		for _, typ := range node.resType {
			fmt.Printf("%.2f ", node.alloc[typ].value)
		}
		fmt.Printf(" | ")
	}
	fmt.Println()
	fmt.Printf("capa: ")
	for _, node := range tCluster.nodes {
		for _, typ := range node.resType {
			fmt.Printf("%.2f ", node.capa[typ].value)
		}
		fmt.Printf(" | ")
	}
	fmt.Println()
	for from, links := range tCluster.links {
		for to, link := range links {
			fmt.Printf("%s->%s: next band alloc:%.2f\n", from, to, link.nextBandAlloc)
		}
	}
	fmt.Println()

	tCluster = clone
	fmt.Printf("max gama: ")
	for _, node := range tCluster.nodes {
		fmt.Printf("%.2f ", node.nextMaxGama)
	}
	fmt.Println()
	fmt.Printf("alloc: ")
	for _, node := range tCluster.nodes {
		for _, typ := range node.resType {
			fmt.Printf("%.2f ", node.alloc[typ].value)
		}
		fmt.Printf("| ")
	}
	fmt.Println()
	fmt.Printf("capa: ")
	for _, node := range tCluster.nodes {
		for _, typ := range node.resType {
			fmt.Printf("%.2f ", node.capa[typ].value)
		}
		fmt.Printf(" | ")
	}
	fmt.Println()
	for from, links := range tCluster.links {
		for to, link := range links {
			fmt.Printf("%s->%s: next band alloc:%.2f\n", from, to, link.nextBandAlloc)
		}
	}
	fmt.Println()
}
