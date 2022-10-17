package scheduler

import (
	"fmt"
	"log"
	"sync"

	"github.com/WeixinX/topology-aware-scheduling-framework/util"
)

//
// appQueue 微服务应用调度优先级队列
//
type appQueue struct {
	mu sync.RWMutex
	pq *util.PriorityQueue
}

func newAppQueue(length int) *appQueue {
	return &appQueue{pq: util.NewPriorityQueue(length)}
}

func (aq *appQueue) push(app *Service) {
	aq.mu.Lock()
	defer aq.mu.Unlock()
	aq.pq.Push(util.NewEntry(app, float32(app.priority)))
}

func (aq *appQueue) pop() *Service {
	aq.mu.Lock()
	defer aq.mu.Unlock()
	return util.ExtractItemFromEntry(aq.pq.Pop()).(*Service)
}

func (aq *appQueue) empty() bool {
	aq.mu.RLock()
	defer aq.mu.RUnlock()
	return aq.pq.Empty()
}

//
// djQueue Dijkstra 算法使用的优先级队列
//
type costPair struct {
	id   nodeId
	cost float32
}

type djQueue struct {
	q *util.PriorityQueue
}

func newDjQueue(length int) *djQueue {
	return &djQueue{q: util.NewPriorityQueue(length)}
}

func (q *djQueue) push(id nodeId, cost float32) {
	// 希望成本越小的越靠前，但在 `util` 中优先级队列的实现是越小越靠后，所以 cost 取负
	q.q.Push(util.NewEntry(costPair{id, cost}, -cost))
}

func (q *djQueue) pop() nodeId {
	return util.ExtractItemFromEntry(q.q.Pop()).(costPair).id
}

func (q *djQueue) empty() bool {
	return q.q.Empty()
}

//
// pathStack 构造最小花费路径用的栈
//
type pathStack struct {
	s *util.Stack
}

func newPathStack() *pathStack {
	return &pathStack{s: util.NewStack()}
}

func (s *pathStack) push(x nodeId) {
	s.s.Push(x)
}

func (s *pathStack) pop() nodeId {
	return s.s.Pop().(nodeId)
}

func (s *pathStack) empty() bool {
	return s.s.Empty()
}

func (s *pathStack) size() int {
	return s.s.Size()
}

//
// DLog debug log
//
var openDebugLog = true

func DLog(level string, format string, v ...interface{}) {
	if openDebugLog {
		log.Printf(fmt.Sprintf("[%s] %s", level, format), v...)
	}
}

func DLogINFO(format string, v ...interface{}) {
	DLog("INFO", format, v...)
}
