package util

import "container/heap"

// PriorityQueue 对内部实现的优先级队列进行包装
type PriorityQueue struct {
	q priorityQueue
}

func NewPriorityQueue(length int) *PriorityQueue {
	pq := &PriorityQueue{q: newPriorityQueue(length)}
	heap.Init(&pq.q)
	return pq
}

func (pq *PriorityQueue) Push(x interface{}) {
	heap.Push(&pq.q, x)
}

func (pq *PriorityQueue) Pop() interface{} {
	return heap.Pop(&pq.q)
}

func (pq *PriorityQueue) Empty() bool {
	return pq.q.empty()
}

// priorityQueue 优先级队列内部实现
type priorityQueue []*entry

type entry struct {
	item     interface{}
	priority float32
}

func (q priorityQueue) Len() int {
	return len(q)
}

func (q priorityQueue) Less(i, j int) bool {
	return q[i].priority > q[j].priority // 优先级越大越靠近队首
}

func (q priorityQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *priorityQueue) Push(x interface{}) {
	e := x.(*entry)
	*q = append(*q, e)
}

func (q *priorityQueue) Pop() interface{} {
	n := q.Len()
	e := (*q)[n-1]
	(*q)[n-1] = nil
	*q = (*q)[:n-1]
	return e
}

func (q *priorityQueue) empty() bool {
	return q.Len() == 0
}

func newPriorityQueue(length int) priorityQueue {
	return make(priorityQueue, 0, length)
}

// NewEntry 暴露创建优先级队列元素接口
func NewEntry(item interface{}, priority float32) *entry {
	return &entry{
		item:     item,
		priority: priority,
	}
}

// ExtractItemFromEntry 暴露从 Entry 中获取 Iterm 的接口
func ExtractItemFromEntry(e interface{}) interface{} {
	return e.(*entry).item
}
