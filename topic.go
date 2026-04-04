package main

type Topic struct {
	topicID uint16
	mq      Queue
}

func (t *Topic) init(tid uint16) {
	t.topicID = tid
	t.mq.init()
}
