package main

import (
	"net"
	"sync"
)

type CGroup struct {
	groupID    uint16
	lock       sync.Mutex
	partitions []Partition
	consumers  []*ConsumerConn
}

type ConsumerConn struct {
	status bool
	conn   net.Conn
}
