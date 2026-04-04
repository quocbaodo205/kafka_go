package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Producer struct {
	port     uint16
	topicID  uint16
}

// Connect to Broker to send register
func (p *Producer) sendPortDataToBroker() error {
	var err error
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	
	pRegMsg := ProducerRegisterMessage{
		port:    p.port,
		topicID: p.topicID,
	}
	fmt.Printf("pRegMsg: port=%d, topicID=%d\n", pRegMsg.port, pRegMsg.topicID)
	message := Message{
		P_REG: &pRegMsg,
	}
	err = writeMessageToStream(streamRW, message)
	if err != nil {
		panic(err)
	}

	// Try to read back from the stream
	resp, err := readMessageFromStream(streamRW)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Receive response from broker: %v\n", *resp.R_P_REG)
	return nil
}

func (p *Producer) startProducerServer() error {
	var err error
	
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", p.port))
	if err != nil {
		panic(err)
	}
	err = p.sendPortDataToBroker()
	if err != nil {
		panic(err)
	}
	conn, _ := ln.Accept() // Block until can
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
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
		// Write PCM
		err = writeMessageToStream(streamRW, Message{
			PCM: []byte(line),
		})
		if err != nil {
			break
		}
		// Try to read back from the stream
		resp, err := readMessageFromStream(streamRW)
		if err != nil {
			break
		}

		fmt.Printf("Receive R_PCM from broker: %d\n", *resp.R_PCM)

	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}
