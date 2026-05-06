package main

import (
	"fmt"
	"net"
	"os"
	"sync"
)

type CGroup struct {
	groupID    uint16
	lock       sync.Mutex
	partitions []Partition
	consumers  []*ConsumerConn

	metaFile *os.File
}

func (cg *CGroup) init(topic_id, group_id uint16) {
	cg.groupID = group_id
	cg.consumers = make([]*ConsumerConn, 0)

	metaFName := fmt.Sprintf("cgroup_metadata_%d_%d.dat", topic_id, group_id)
	// With topic_id and group_id, read back the metadata file and read back all partition.

	// [size:u32 : how many partition are there]
	var err error
	// File for head and tail of this partition
	cg.metaFile, err = os.OpenFile(metaFName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	size := int(readu32(cg.metaFile, 0))
	cg.partitions = make([]Partition, size)
	for i := 0; i < size; i += 1 {
		cg.partitions[i] = Partition{}
		cg.partitions[i].init(topic_id, group_id, uint16(i+1))
	}
}

func (cg *CGroup) store() {
	writeu32(cg.metaFile, 0, uint32(len(cg.partitions)))
	sz := readu32(cg.metaFile, 0)
	fmt.Printf("debug metadata file name = %s: sz = %d\n", cg.metaFile.Name(), sz)
}

type ConsumerConn struct {
	status bool
	conn   net.Conn
}
