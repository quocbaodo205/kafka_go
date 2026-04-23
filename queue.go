package main

import "fmt"

const (
	maxMessageSize = 255
	queueCapacity  = 10000
)

// var underArr [maxMessageSize * queueCapacity]byte;
var underArr = make([]byte, maxMessageSize*queueCapacity)
var underSize = make([]byte, maxMessageSize*queueCapacity)

type Queue struct {
	head uint32
	tail uint32
}

func (q *Queue) init() {
	q.head = 0
	q.tail = 0
}

// Assume data length <= maxMessageSize
func (q *Queue) push(data []byte) {
	copy(underArr[q.tail:int(q.tail)+len(data)], data)
	underSize[q.tail] = byte(len(data))
	q.tail += maxMessageSize
	q.tail %= maxMessageSize * queueCapacity
}

func (q *Queue) pop() []byte {
	if q.head == q.tail {
		return nil
	}
	data := underArr[q.head : q.head+uint32(underSize[q.head])]
	q.head += maxMessageSize
	q.head %= maxMessageSize * queueCapacity
	return data
}

func (q *Queue) peek(offset uint) []byte {
	if q.head == q.tail {
		return nil
	}
	position := q.head + uint32(offset*maxMessageSize)
	position %= maxMessageSize * queueCapacity
	if q.head < q.tail {
		if !(position >= q.head && position < q.tail) {
			return nil
		}
	} else {
		if !(position >= q.head || position < q.tail) {
			return nil
		}
	}
	data := underArr[position : position+uint32(underSize[position])]
	return data
}

func (q *Queue) size() int {
	if q.tail >= q.head {
		return int((q.tail - q.head) / maxMessageSize)
	} else {
		return int(((maxMessageSize*queueCapacity - q.head) + q.tail) / maxMessageSize)
	}
}

func (q *Queue) debug() {
	fmt.Printf("Debug queue: \n")
	var cur = q.head
	for {
		data := underArr[cur : cur+uint32(underSize[cur])]
		fmt.Printf("%s\n", data)
		cur += maxMessageSize
		cur %= maxMessageSize * queueCapacity
		if cur == q.tail {
			break
		}
	}
}
