package main

import "sync"

type Partition struct {
	queue Queue
	lock  sync.Mutex
}

func (p *Partition) init(topic_id, cgroup_id, partition_id uint16) {
	p.queue = Queue{}
	p.queue.init(topic_id, cgroup_id, partition_id)
}
