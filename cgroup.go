package main

import (
	"net"
	"sync"
)

type CGroup struct {
	groupID   uint16
	offset    uint
	consumers []ConsumerConn
	lock      sync.Mutex

	readyConsumers []*ConsumerConn
}

type ConsumerConn struct {
	status bool
	conn   net.Conn
}
