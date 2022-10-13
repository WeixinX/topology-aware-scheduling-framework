package util

import (
	"fmt"
	"testing"
)

type fruit struct {
	name     string
	priority int
}

func TestPriorityQueue(t *testing.T) {
	f := []fruit{
		{name: "apple", priority: 2},
		{name: "orange", priority: 3},
		{name: "banana", priority: 4},
		{name: "pear", priority: 5},
	}

	q := NewPriorityQueue(5)
	q.Push(NewEntry(f[0], float32(f[0].priority))) // apple
	q.Push(NewEntry(f[2], float32(f[2].priority))) // banana
	q.Push(NewEntry(f[3], float32(f[3].priority))) // pear
	q.Pop()                                        // pop "pear"
	q.Push(NewEntry(f[1], float32(f[1].priority))) // pear

	for !q.Empty() {
		e := ExtractItemFromEntry(q.Pop()).(fruit)
		fmt.Println(e.name, e.priority)
		// banana 4
		// orange 3
		// apple 2
	}
}
