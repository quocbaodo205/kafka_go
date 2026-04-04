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
	data := underArr[q.head : q.head+uint32(underSize[q.head])]
	q.head += maxMessageSize
	q.head %= maxMessageSize * queueCapacity
	return data
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
