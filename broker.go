package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

const BROKER_PORT = 10000

type Broker struct {
	topics []*Topic
}

func (b *Broker) init() {
	b.topics = make([]*Topic, 0)
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
	if message.C_REG != nil {
		resp, err := b.processConsumerRegisterMessage(*message.C_REG)
		if err != nil {
			return nil, err
		}
		return &Message{R_C_REG: resp}, nil
	}
	return nil, nil
}

func (b *Broker) processProducerPCM(pcm []byte, topic *Topic) (byte, error) {
	topic.mq.push(pcm)
	topic.mq.debug()
	return 0, nil
}

func (b *Broker) processEchoMessage(echoMessage *string) (string, error) {
	return fmt.Sprintf("I have receiver: %s", *echoMessage), nil
}

func (b *Broker) processProducerRegisterMessage(pRegMessage ProducerRegisterMessage) (*byte, error) {
	fmt.Printf("Broker received pRegMessage: port=%d, topicID=%d\n", pRegMessage.port, pRegMessage.topicID)
	var topic *Topic
	for _, tp := range b.topics {
		if tp.topicID == pRegMessage.topicID {
			topic = tp
			break
		}
	}
	if topic == nil {
		tp := &Topic{}
		tp.init(pRegMessage.topicID)
		b.topics = append(b.topics, tp)
		topic = tp
		go b.stopAndPop(topic)
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
				resp, err := b.processProducerPCM(parsedMessage.PCM, topic)
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

func (b *Broker) stopAndPop(t *Topic) {
	for {
		time.Sleep(5 * time.Second)
		t.lock.Lock()
		minOffset := -1
		for _, cg := range t.cgroups {
			if minOffset == -1 {
				minOffset = int(cg.offset)
			} else {
				if cg.offset < uint(minOffset) {
					minOffset = int(cg.offset)
				}
			}
		}
		fmt.Printf("Stop and pop run, minOffset = %d\n", minOffset)
		if minOffset != -1 {
			for _, cg := range t.cgroups {
				cg.lock.Lock()
				cg.offset -= uint(minOffset)
			}
			for {
				if minOffset == 0 {
					break
				}
				t.mq.pop()
				minOffset -= 1
			}
			for _, cg := range t.cgroups {
				cg.lock.Unlock()
			}
		}
		t.lock.Unlock()
	}
}

func (b *Broker) processConsumerRegisterMessage(cRegMessage ConsumerRegisterMessage) (*byte, error) {
	fmt.Printf("Broker received cRegMessage: port=%d, topicID=%d, groupID=%d\n", cRegMessage.port, cRegMessage.topicID, cRegMessage.groupID)
	var topic *Topic
	for _, tp := range b.topics {
		if tp.topicID == cRegMessage.topicID {
			topic = tp
			break
		}
	}
	if topic == nil {
		tp := &Topic{}
		tp.init(cRegMessage.topicID)
		b.topics = append(b.topics, tp)
		topic = tp
	}
	var cgroup *CGroup
	for _, cg := range topic.cgroups {
		if cg.groupID == cRegMessage.groupID {
			cgroup = cg
			break
		}
	}
	if cgroup == nil {
		cg := &CGroup{
			groupID: cRegMessage.groupID,
			offset:  0,
		}
		topic.lock.Lock()
		topic.cgroups = append(topic.cgroups, cg)
		topic.lock.Unlock()
		cgroup = cg
		// go b.startConsumerGroupConsumption(topic, cgroup)
	}
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", cRegMessage.port))
	fmt.Printf("Connected to consumer at port %v\n", cRegMessage.port)
	consumer := ConsumerConn{
		status: true,
		conn:   conn,
	}
	cgroup.lock.Lock()
	cgroup.consumers = append(cgroup.consumers, consumer)
	fmt.Printf("Pushed to the list of consumer, port %v\n", cRegMessage.port)
	cgroup.lock.Unlock()
	// go b.readConsumerReady(cgroup, &consumer)
	go b.readConsumerReadyAndSend(topic, cgroup, &consumer)
	var resp byte = 0
	return &resp, nil
}

func (b *Broker) readConsumerReadyAndSend(topic *Topic, cgroup *CGroup, consumerConn *ConsumerConn) {
	streamRW := bufio.NewReadWriter(bufio.NewReader(consumerConn.conn), bufio.NewWriter(consumerConn.conn))

	for {
		// Read ack
		parsedMessage, err := readMessageFromStream(streamRW) // Wait forever!!
		if parsedMessage == nil || err != nil {
			panic(err)
		}
		if parsedMessage.R_PCM != nil {
			consumerConn.status = true
		} else {
			fmt.Printf("Parsed message not R_PCM: %v", parsedMessage)
			panic("Why not R_PCM???")
		}

		cgroup.lock.Lock()
		offset := cgroup.offset
		// Take message from topic for consumption
		pcm := topic.mq.peek(offset)
		// fmt.Printf("offset = %d, pcm = %v\n", offset, pcm)
		// time.Sleep(5 * time.Second)
		if pcm == nil {
			cgroup.lock.Unlock()
			continue
		}

		// Write PCM message to ready consumer
		consumerConn.status = false
		err = writeMessageToStream(streamRW, Message{
			PCM: pcm,
		})
		if err != nil {
			panic(err)
		}

		// Increase offset on consumed
		cgroup.offset += 1
		cgroup.lock.Unlock()
	}
}
