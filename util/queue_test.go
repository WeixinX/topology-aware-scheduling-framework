package util

import (
	"fmt"
	"testing"
)

func TestQueue(t *testing.T) {
	f := []*fruit{
		{name: "apple", priority: 1},
		{name: "orange", priority: 2},
		{name: "banana", priority: 3},
		{name: "pear", priority: 4},
	}

	q := NewQueue(5)
	for _, iterm := range f {
		q.Push(iterm)
	}
	for !q.Empty() {
		h := q.Pop().(*fruit)
		fmt.Println(h.name, h.priority)
	}
}
