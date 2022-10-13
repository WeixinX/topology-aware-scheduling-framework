package scheduler

import (
	"fmt"
	"testing"
	"time"
)

func TestMOTAS(t *testing.T) {
	app := tService
	cluster := tCluster
	mts := NewMOTAS(cluster)
	mts.AddTask(app)
	time.Sleep(5 * time.Second)
	mts.Kill()
}

func TestMsQueue(t *testing.T) {
	// priority: 3(5) > 5(4) > 2(3) > 4(2) > 1(1)
	apps := []*Service{
		&Service{
			id:       "1",
			priority: 1,
		},
		&Service{
			id:       "2",
			priority: 3,
		},
		&Service{
			id:       "3",
			priority: 5,
		},
		&Service{
			id:       "4",
			priority: 2,
		},
		&Service{
			id:       "5",
			priority: 4,
		},
	}
	appq := newAppQueue(5)
	for _, app := range apps {
		appq.push(app)
	}
	for !appq.empty() {
		app := appq.pop()
		fmt.Println(app.id, app.priority)
	}
}

func TestTravel(t *testing.T) {
	//             |-> 3
	//     |-> 1 ->
	//     |       |-> 4
	// 0 ->
	//     |
	//     |-> 2 -> 5
	app := &Service{
		rootId: "0",
		ms: map[msId]*Microservice{
			"0": &Microservice{},
			"1": &Microservice{},
			"2": &Microservice{},
			"3": &Microservice{},
			"4": &Microservice{},
			"5": &Microservice{},
		},
		dep: map[msId][]*Dependence{
			"0": []*Dependence{
				{umId: "0", dmId: "1"},
				{umId: "0", dmId: "2"},
			},
			"1": []*Dependence{
				{umId: "1", dmId: "3"},
				{umId: "1", dmId: "4"},
			},
			"2": []*Dependence{
				{umId: "2", dmId: "5"},
			},
		},
		reDep: map[msId][]*Dependence{
			"5": []*Dependence{
				{umId: "2", dmId: "5"},
			},
			"4": []*Dependence{
				{umId: "1", dmId: "4"},
			},
			"3": []*Dependence{
				{umId: "1", dmId: "3"},
			},
			"2": []*Dependence{
				{umId: "0", dmId: "2"},
			},
			"1": []*Dependence{
				{umId: "0", dmId: "1"},
			},
		},
	}
	order := app.getLevelOrder()
	for _, id := range order {
		fmt.Printf("%s ", id)
	}
	fmt.Println()

	order = app.getTopologyOrder()
	for _, id := range order {
		fmt.Printf("%s ", id)
	}
	fmt.Println()
}

func TestSomething(t *testing.T) {
	type test struct {
		id    string
		value int
	}
	m := map[string]*test{
		"w": {"w", 18},
		"x": {"x", 22},
	}
	m2 := make(map[string]*test, len(m))
	m3 := make(map[string]*test, len(m))
	for k, v := range m {
		m2[k] = v

		vv := *v
		m3[k] = &vv
	}
	m3["w"].value = 50
	for k, v := range m {
		fmt.Println(k, v.value)
	}

	m2["w"].value = 50
	for k, v := range m {
		fmt.Println(k, v.value)
	}

	if te, ok := m["x"]; ok {
		te.value = 1
	}
	fmt.Println(m["x"].value)
}
