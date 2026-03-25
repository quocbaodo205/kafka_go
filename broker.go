package main

import (
	"bufio"
	"fmt"
	"net"
)

const BROKER_PORT = 10000

const (
	ECHO = 1
	// Other message types
)

type Message struct {
	ECHO *string
	// Other type here...
}

func readFromStream(stream_rw *bufio.ReadWriter) ([]byte, error) {
	var err error
	// Read
	header, err := stream_rw.ReadByte() // Block
	if err != nil {
		return nil, err
	}

	data, err := stream_rw.Peek(int(header)) // Block
	if err != nil {
		return nil, err
	}

	_, err = stream_rw.Discard(int(header))
	if err != nil {
		return nil, err
	}

	return data, err
}

func writeToStream(stream_rw *bufio.ReadWriter, data string) error {
	var err error
	// Write
	err = stream_rw.WriteByte(byte(len(data)))
	if err != nil {
		return err
	}
	_, err = stream_rw.WriteString(data)
	if err != nil {
		return err
	}
	stream_rw.Flush()

	return nil
}

type Broker struct {
}

func (b *Broker) startBrokerServer() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		panic(err)
	}
	for {
		conn, _ := ln.Accept() // Block until can
		stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

		var err error
		data, err := readFromStream(stream_rw)
		if err != nil {
			return err
		}

		// Process
		parsed_message := b.parseBrokerMessage(data)
		if parsed_message != nil {
			resp, err := b.processBrokerMessage(parsed_message)
			if err != nil {
				return err
			}
			// Write it back
			err = writeToStream(stream_rw, resp)
			if err != nil {
				return err
			}
		}

		err = conn.Close()
		if err != nil {
			return err
		}
	}
}

func (b *Broker) parseBrokerMessage(message []byte) *Message {
	switch message[0] {
	case ECHO:
		var st = string(message[1:])
		return &Message{ECHO: &st}
	default:
		return nil
	}
}

func (b *Broker) processBrokerMessage(message *Message) (string, error) {
	var err error
	var resp string

	if message.ECHO != nil {
		resp = fmt.Sprintf("I have receiver: %s", *message.ECHO)
	}
	return resp, err
}
