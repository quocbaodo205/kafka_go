package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

type Consumer struct {
	port    uint16
	topicID uint16
	groupID uint16
}

// Connect to Broker to send register
func (c *Consumer) sendPortDataToBroker() error {
	var err error
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	cRegMsg := ConsumerRegisterMessage{
		port:    c.port,
		topicID: c.topicID,
		groupID: c.groupID,
	}
	fmt.Printf("cRegMsg: port=%d, topicID=%d, groupID=%d\n", cRegMsg.port, cRegMsg.topicID, cRegMsg.groupID)
	message := Message{
		C_REG: &cRegMsg,
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

	fmt.Printf("Receive response from broker: %v\n", *resp.R_C_REG)
	return nil
}

func (c *Consumer) startConsumerServer() error {
	var err error

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", c.port))
	if err != nil {
		panic(err)
	}
	err = c.sendPortDataToBroker()
	if err != nil {
		panic(err)
	}
	conn, _ := ln.Accept() // Block until can
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	fmt.Printf("Started consumer server, receiving...\n")

	for {
		// Read message to consume
		message, err := readMessageFromStream(streamRW)
		if err != nil {
			break
		}
		fmt.Printf("Receive PCM from broker: %s\n", message.PCM)
		time.Sleep(1 * time.Second)
		// Write R_PCM
		var resp byte = 1
		err = writeMessageToStream(streamRW, Message{
			R_PCM: &resp,
		})
		if err != nil {
			break
		}
	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}
