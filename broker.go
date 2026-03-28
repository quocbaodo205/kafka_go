package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

const BROKER_PORT = 10000

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
		parsed_message, err := readMessageFromStream(stream_rw)

		// Process
		if err == nil && parsed_message != nil {
			resp, err := b.processBrokerMessage(parsed_message)
			if err != nil {
				return err
			}
			// Write it back
			err = writeMessageToStream(stream_rw, *resp)
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

// Process:
// - Call inner process function for each message type
// - Response correct Message
func (b *Broker) processBrokerMessage(message *Message) (*Message, error) {
	if message.ECHO != nil {
		resp, err := b.processEchoMessage(message.ECHO)
		if err != nil {
			return nil, err
		}
		return &Message{R_ECHO: &resp}, nil
	}
	if message.P_REG != nil {
		resp, err := b.processProducerRegisterMessage(message.P_REG)
		if err != nil {
			return nil, err
		}
		return &Message{R_P_REG: resp}, nil
	}
	return nil, nil
}

func (b *Broker) processEchoMessage(echo_message *string) (string, error) {
	return fmt.Sprintf("I have receiver: %s", *echo_message), nil
}

func (b *Broker) processProducerRegisterMessage(p_reg_message *string) (*byte, error) {
	port, err := strconv.ParseInt(*p_reg_message, 10, 32)
	if err != nil {
		return nil, err
	}
	go func() {
		conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", port))
		fmt.Printf("Connected to server at port %v\n", port)
		// Read input from stdin and write to stream.
		stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			parsed_message, err := readMessageFromStream(stream_rw)
			if parsed_message == nil || err != nil {
				panic(err)
			}
			// Process something here
			resp, err := b.processBrokerMessage(parsed_message)
			if err != nil {
				panic(err)
			}
			err = writeMessageToStream(stream_rw, *resp)
			if err != nil {
				panic(err)
			}
		}
	}()
	var resp byte = 0
	return &resp, err
}
