package util

type Queue struct {
	iterm []interface{}
}

func NewQueue(length int) *Queue {
	return &Queue{iterm: make([]interface{}, 0, length)}
}

func (q *Queue) Push(x interface{}) {
	q.iterm = append(q.iterm, x)
}

func (q *Queue) Pop() interface{} {
	if q.Empty() {
		return nil
	}
	head := q.iterm[0]
	q.iterm[0] = nil
	q.iterm = q.iterm[1:]
	return head
}

func (q *Queue) Empty() bool {
	return len(q.iterm) == 0
}
