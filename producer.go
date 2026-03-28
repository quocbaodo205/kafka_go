package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Producer struct {
}

// Connect to Broker to send register
func (b *Producer) sendPortDataToBroker(port int16) error {
	var err error
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	port_str := fmt.Sprintf("%d", port)
	message := Message{
		P_REG: &port_str,
	}
	err = writeMessageToStream(stream_rw, message)
	if err != nil {
		panic(err)
	}

	// Try to read back from the stream
	resp, err := readMessageFromStream(stream_rw)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Receive response from broker: %v\n", *resp.R_P_REG)
	return nil
}

func (b *Producer) startProducerServer(port int16) error {
	var err error

	err = b.sendPortDataToBroker(port)
	if err != nil {
		panic(err)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
	conn, _ := ln.Accept() // Block until can
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	rd := bufio.NewReader(os.Stdin)

	for {
		// Read from stdin
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				// Probably panic here
			}
		}
		// Write ECHO
		err = writeMessageToStream(stream_rw, Message{
			ECHO: &line,
		})
		if err != nil {
			break
		}
		// Try to read back from the stream
		resp, err := readMessageFromStream(stream_rw)
		if err != nil {
			break
		}

		fmt.Printf("Receive message from broker: %s\n", *resp.R_ECHO)

	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}
