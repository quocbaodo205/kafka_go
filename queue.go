package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/tidwall/mmap"
)

const (
	maxMessageSize = 255
	queueCapacity  = 10000
)

type Queue struct {
	head uint32
	tail uint32

	underArr  []byte
	underSize []byte

	metaFile *os.File
}

func readu32(f *os.File, off int64) uint32 {
	buf := make([]byte, 4)
	f.ReadAt(buf, off)
	x := binary.BigEndian.Uint32(buf)
	return x
}

func writeu32(f *os.File, off int64, x uint32) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, x)
	f.WriteAt(buf, off)
}

func readu16(f *os.File, off int64) uint16 {
	buf := make([]byte, 2)
	f.ReadAt(buf, off)
	x := binary.BigEndian.Uint16(buf)
	return x
}

func writeu16(f *os.File, off int64, x uint16) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, x)
	f.WriteAt(buf, off)
}

func debugMetadata(f *os.File) {
	head := readu32(f, 0)
	tail := readu32(f, 4)
	fmt.Printf("debug metadata file name = %s: head = %d, tail = %d\n", f.Name(), head, tail)
}

func isExist(path string) bool {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func (q *Queue) init(topic_id, cgroup_id, partition_id uint16) {
	q.head = 0
	q.tail = 0

	metaFName := fmt.Sprintf("partition_metadata_%d_%d_%d.dat", topic_id, cgroup_id, partition_id)
	underArrFName := fmt.Sprintf("underArr_%d_%d_%d.dat", topic_id, cgroup_id, partition_id)
	underSizeFName := fmt.Sprintf("underSize_%d_%d_%d.dat", topic_id, cgroup_id, partition_id)

	var err error
	// File for head and tail of this partition
	q.metaFile, err = os.OpenFile(metaFName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	q.head = readu32(q.metaFile, 0)
	q.tail = readu32(q.metaFile, 4)

	if isExist(underArrFName) {
		fmt.Printf("File %s exist, mmap Open instead of Create\n", underArrFName)
		q.underArr, err = mmap.Open(underArrFName, true)
		if err != nil {
			panic(err)
		}

		q.underSize, err = mmap.Open(underSizeFName, true)
		if err != nil {
			panic(err)
		}

	} else {
		q.underArr, err = mmap.Create(underArrFName, maxMessageSize*queueCapacity)
		if err != nil {
			panic(err)
		}

		q.underSize, err = mmap.Create(underSizeFName, maxMessageSize*queueCapacity)
		if err != nil {
			panic(err)
		}
	}
}

func (q *Queue) deinit() {
	mmap.Close(q.underArr)
	mmap.Close(q.underSize)
}

// Assume data length <= maxMessageSize
func (q *Queue) push(data []byte) {
	copy(q.underArr[q.tail:int(q.tail)+len(data)], data)
	q.underSize[q.tail] = byte(len(data))
	q.tail += maxMessageSize
	q.tail %= maxMessageSize * queueCapacity

	writeu32(q.metaFile, 4, q.tail)
	debugMetadata(q.metaFile)
}

func (q *Queue) pop() []byte {
	if q.head == q.tail {
		return nil
	}
	data := q.underArr[q.head : q.head+uint32(q.underSize[q.head])]
	q.head += maxMessageSize
	q.head %= maxMessageSize * queueCapacity

	writeu32(q.metaFile, 0, q.head)
	debugMetadata(q.metaFile)
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
	data := q.underArr[position : position+uint32(q.underSize[position])]
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
		data := q.underArr[cur : cur+uint32(q.underSize[cur])]
		fmt.Printf("%s\n", data)
		cur += maxMessageSize
		cur %= maxMessageSize * queueCapacity
		if cur == q.tail {
			break
		}
	}
}
