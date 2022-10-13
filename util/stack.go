package util

type Stack []interface{}

func NewStack() *Stack {
	return &Stack{}
}

func (s *Stack) Push(x interface{}) {
	*s = append(*s, x)
}

func (s *Stack) Pop() interface{} {
	if s.Empty() {
		return nil
	}

	n := len(*s)
	ret := (*s)[n-1]
	(*s)[n-1] = nil
	*s = (*s)[:n-1]
	return ret
}

func (s *Stack) Top() interface{} {
	if s.Empty() {
		return nil
	}

	return (*s)[len(*s)-1]
}

func (s *Stack) Empty() bool {
	return s.Size() == 0
}

func (s *Stack) Size() int {
	return len(*s)
}
