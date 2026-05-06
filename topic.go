package main

import (
	"fmt"
	"os"
	"sync"
)

type Topic struct {
	topicID uint16
	mq      Queue
	cgroups []*CGroup
	lock    sync.Mutex

	metaFile *os.File
}

func (t *Topic) init(tid uint16) {
	t.topicID = tid
	t.mq.init(tid, uint16(65535), uint16(65535))

	metaFName := fmt.Sprintf("topic_metadata_%d.dat", tid)
	var err error
	t.metaFile, err = os.OpenFile(metaFName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	size := int(readu32(t.metaFile, 0))
	t.cgroups = make([]*CGroup, 0, size)
	for i := 0; i < size; i += 1 {
		groupID := readu16(t.metaFile, int64(4+i*2))
		cg := &CGroup{}
		cg.init(tid, groupID)
		t.cgroups = append(t.cgroups, cg)
	}
	fmt.Printf("debug metadata file name = %s: cgroups = %d\n", t.metaFile.Name(), size)
}

func (t *Topic) store() {
	writeu32(t.metaFile, 0, uint32(len(t.cgroups)))
	for i, cgroup := range t.cgroups {
		writeu16(t.metaFile, int64(4+i*2), cgroup.groupID)
	}
	fmt.Printf("debug metadata file name = %s: cgroups = %d\n", t.metaFile.Name(), len(t.cgroups))
}
