package main

import "sync"

type Topic struct {
	topicID uint16
	mq      Queue
	cgroups []*CGroup
	lock    sync.Mutex
}

func (t *Topic) init(tid uint16) {
	t.topicID = tid
	t.mq.init()
	t.cgroups = make([]*CGroup, 0)
}
