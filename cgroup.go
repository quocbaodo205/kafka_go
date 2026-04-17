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
}

type ConsumerConn struct {
	status bool
	conn   net.Conn
}
