package main

import (
	"bufio"
	"fmt"
	"net"
)

const BROKER_PORT = 10000

type Broker struct {
	topics []Topic
}

func (b *Broker) init() {
	b.topics = make([]Topic, 0)
}

func (b *Broker) startBrokerServer() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		panic(err)
	}
	for {
		conn, _ := ln.Accept() // Block until can
		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

		var err error
		parsedMessage, err := readMessageFromStream(streamRW)

		// Process
		if err == nil && parsedMessage != nil {
			resp, err := b.processBrokerMessage(parsedMessage)
			if err != nil {
				return err
			}
			// Write it back
			err = writeMessageToStream(streamRW, *resp)
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
		resp, err := b.processProducerRegisterMessage(*message.P_REG)
		if err != nil {
			return nil, err
		}
		return &Message{R_P_REG: resp}, nil
	}
	return nil, nil
}

func (b *Broker) processProducerPCM(pcm []byte, topicIdx int) (byte, error) {
	b.topics[topicIdx].mq.push(pcm)
	b.topics[topicIdx].mq.debug()
	return 0, nil
}

func (b *Broker) processEchoMessage(echoMessage *string) (string, error) {
	return fmt.Sprintf("I have receiver: %s", *echoMessage), nil
}

func (b *Broker) processProducerRegisterMessage(pRegMessage ProducerRegisterMessage) (*byte, error) {
	fmt.Printf("Broker received pRegMessage: port=%d, topicID=%d\n", pRegMessage.port, pRegMessage.topicID)
	var topicIdx int = -1
	for idx, tp := range b.topics {
		if tp.topicID == pRegMessage.topicID {
			topicIdx = idx
			break
		}
	}
	if topicIdx == -1 {
		tp := Topic{}
		tp.init(pRegMessage.topicID)
		b.topics = append(b.topics, tp)
		topicIdx = len(b.topics) - 1
	}
	go func() {
		conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", pRegMessage.port))
		fmt.Printf("Connected to server at port %v\n", pRegMessage.port)
		// Read input from stdin and write to stream.
		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			parsedMessage, err := readMessageFromStream(streamRW)
			if parsedMessage == nil || err != nil {
				panic(err)
			}
			// Process something here
			if parsedMessage.PCM != nil {
				resp, err := b.processProducerPCM(parsedMessage.PCM, topicIdx)
				if err != nil {
					panic(err)
				}
				err = writeMessageToStream(streamRW, Message{
					R_PCM: &resp,
				})
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	var resp byte = 0
	return &resp, nil
}
