package main

import "sync"

type Partition struct {
	queue Queue
	lock  sync.Mutex
}

func (p *Partition) init() {
	p.queue = Queue{}
	p.queue.init()
}
