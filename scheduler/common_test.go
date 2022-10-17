package scheduler

import "math"

var (
	DefaultResType = []ResourceType{ResCPU, ResMem}
	DefaultResReq  = map[ResourceType]Resource{
		ResCPU: {ResCPU, 2},       // cores
		ResMem: {ResMem, 25 * MB}, // MB
	}
	DefaultBandReq float32 = 15 * MB

	DefaultResCPU float32 = 8
	DefaultResMem float32 = 120 * MB
	DefaultBrand  float32 = 30 * MB
)

func newTestService(resReq map[ResourceType]Resource, bandReq float32) *Service {
	return &Service{
		id:     "test0",
		rootId: "A",
		ms: map[msId]*Microservice{
			"A": {
				id:            "A",
				resReq:        resReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"B": {
				id:            "B",
				resReq:        resReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"C": {
				id:            "C",
				resReq:        resReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"D": {
				id:            "D",
				resReq:        resReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"E": {
				id:            "E",
				resReq:        resReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
			"F": {
				id:            "F",
				resReq:        resReq,
				placeNode:     NotPlaced,
				nextPlaceNode: NotPlaced,
			},
		},
		dep: map[msId][]*Dependence{
			"A": {
				{"A", "B", bandReq},
				{"A", "C", bandReq},
			},
			"B": {
				{"B", "D", bandReq},
				{"B", "E", bandReq},
			},
			"C": {
				{"C", "D", bandReq},
				{"C", "F", bandReq},
			},
		},
		reDep: map[msId][]*Dependence{
			"B": {
				{"A", "B", bandReq},
			},
			"C": {
				{"A", "C", bandReq},
			},
			"D": {
				{"B", "D", bandReq},
				{"C", "D", bandReq},
			},
			"E": {
				{"B", "E", bandReq},
			},
			"F": {
				{"C", "F", bandReq},
			},
		},
		priority: 5,
	}
}

func newTestCluster(resType []ResourceType, resCPU, resMem, band float32) *Cluster {
	return &Cluster{
		nodes: map[nodeId]*Node{
			"node0": {
				id:          "node0",
				resType:     resType,
				capa:        map[ResourceType]*Resource{ResCPU: {ResCPU, resCPU}, ResMem: {ResMem, resMem}},
				alloc:       map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				nextAlloc:   map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				args:        map[ResourceType]float32{ResCPU: 0.5, ResMem: 0.5},
				minGama:     math.MaxFloat32,
				nextMinGama: math.MaxFloat32,
				threshold:   0.8,
			},
			"node1": {
				id:          "node1",
				resType:     resType,
				capa:        map[ResourceType]*Resource{ResCPU: {ResCPU, resCPU}, ResMem: {ResMem, resMem}},
				alloc:       map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				nextAlloc:   map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				args:        map[ResourceType]float32{ResCPU: 0.5, ResMem: 0.5},
				minGama:     math.MaxFloat32,
				nextMinGama: math.MaxFloat32,
				threshold:   0.8,
			},
			"node2": {
				id:          "node2",
				resType:     resType,
				capa:        map[ResourceType]*Resource{ResCPU: {ResCPU, resCPU}, ResMem: {ResMem, resMem}},
				alloc:       map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				nextAlloc:   map[ResourceType]*Resource{ResCPU: {ResCPU, 0}, ResMem: {ResMem, 0}},
				args:        map[ResourceType]float32{ResCPU: 0.5, ResMem: 0.5},
				minGama:     math.MaxFloat32,
				nextMinGama: math.MaxFloat32,
				threshold:   0.8,
			},
			"node3": {
				id:          "node3",
				resType:     resType,
				capa:        map[ResourceType]*Resource{ResCPU: {ResCPU, resCPU}, ResMem: {ResMem, resMem}},
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
				"node0": &Link{
					from:    "node0",
					to:      "node0",
					cost:    0,
					bandCap: band,
				},
				"node1": &Link{
					from:    "node0",
					to:      "node1",
					cost:    1,
					bandCap: band,
				},
				"node2": &Link{
					from:    "node0",
					to:      "node2",
					cost:    1,
					bandCap: band,
				},
				"node3": &Link{
					from:    "node0",
					to:      "node3",
					cost:    1,
					bandCap: band,
				},
			},
			"node1": {
				"node0": &Link{
					from:    "node1",
					to:      "node0",
					cost:    1,
					bandCap: band,
				},
				"node1": &Link{
					from:    "node1",
					to:      "node1",
					cost:    0,
					bandCap: band,
				},
				"node2": &Link{
					from:    "node1",
					to:      "node2",
					cost:    1,
					bandCap: band,
				},
				"node3": &Link{
					from:    "node1",
					to:      "node3",
					cost:    1,
					bandCap: band,
				},
			},
			"node2": {
				"node0": &Link{
					from:    "node2",
					to:      "node0",
					cost:    1,
					bandCap: band,
				},
				"node1": &Link{
					from:    "node2",
					to:      "node1",
					cost:    1,
					bandCap: band,
				},
				"node2": &Link{
					from:    "node2",
					to:      "node2",
					cost:    0,
					bandCap: band,
				},
				"node3": &Link{
					from:    "node2",
					to:      "node3",
					cost:    1,
					bandCap: band,
				},
			},
			"node3": {
				"node0": &Link{
					from:    "node3",
					to:      "node0",
					cost:    1,
					bandCap: band,
				},
				"node1": &Link{
					from:    "node3",
					to:      "node1",
					cost:    1,
					bandCap: band,
				},
				"node2": &Link{
					from:    "node3",
					to:      "node2",
					cost:    1,
					bandCap: band,
				},
				"node3": &Link{
					from:    "node3",
					to:      "node3",
					cost:    0,
					bandCap: band,
				},
			},
		},
	}
}
