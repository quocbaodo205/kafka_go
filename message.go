package main

import (
	"bufio"
	"fmt"
)

const (
	ECHO  = 1
	P_REG = 2
	C_REG = 3
	PCM   = 4
	// Response
	R_ECHO  = 101
	R_P_REG = 102
	R_C_REG = 103
	R_PCM   = 104
)

type Message struct {
	ECHO  *string
	P_REG *ProducerRegisterMessage
	C_REG *ConsumerRegisterMessage
	PCM   []byte // nil-able
	// Response
	R_ECHO  *string
	R_P_REG *byte
	R_C_REG *byte
	R_PCM   *byte
}

type ProducerRegisterMessage struct {
	port    uint16
	topicID uint16
}

func (m *ProducerRegisterMessage) fromByte(streamMessage []byte) {
	// First 2 bytes: port
	// Next 2 bytes: topicID
	m.port = uint16(streamMessage[0])<<8 + uint16(streamMessage[1])
	m.topicID = uint16(streamMessage[2])<<8 + uint16(streamMessage[3])
}

func (m *ProducerRegisterMessage) toByte() []byte {
	var data [4]byte
	// First 2 bytes: port
	// Next 2 bytes: topicID

	// 244 131
	data[0] = byte(m.port >> 8)
	data[1] = byte(m.port % 256)

	data[2] = byte(m.topicID >> 8)
	data[3] = byte(m.topicID % 256)
	return data[0:4]
}

type ConsumerRegisterMessage struct {
	port    uint16
	topicID uint16
	groupID uint16
}

func (m *ConsumerRegisterMessage) fromByte(streamMessage []byte) {
	// First 2 bytes: port
	// Next 2 bytes: topicID
	// Next 2 bytes: groupID
	m.port = uint16(streamMessage[0])<<8 + uint16(streamMessage[1])
	m.topicID = uint16(streamMessage[2])<<8 + uint16(streamMessage[3])
	m.groupID = uint16(streamMessage[4])<<8 + uint16(streamMessage[5])
}

func (m *ConsumerRegisterMessage) toByte() []byte {
	var data [6]byte
	// First 2 bytes: port
	// Next 2 bytes: topicID
	// Next 2 bytes: groupID

	data[0] = byte(m.port >> 8)
	data[1] = byte(m.port % 256)

	data[2] = byte(m.topicID >> 8)
	data[3] = byte(m.topicID % 256)

	data[4] = byte(m.groupID >> 8)
	data[5] = byte(m.groupID % 256)
	return data[0:6]
}

// Message format:
// - stream[0]: size
// -> stream[1:]: []byte
func readFromStream(streamRW *bufio.ReadWriter) ([]byte, error) {
	var err error
	// Read
	header, err := streamRW.ReadByte() // Block
	if err != nil {
		return nil, err
	}

	data, err := streamRW.Peek(int(header)) // Block
	if err != nil {
		return nil, err
	}

	_, err = streamRW.Discard(int(header))
	if err != nil {
		return nil, err
	}

	return data, err
}

func parseMessage(streamMessage []byte) *Message {
	switch streamMessage[0] {
	case ECHO:
		var st = string(streamMessage[1:])
		return &Message{ECHO: &st}
	case R_ECHO:
		var st = string(streamMessage[1:])
		return &Message{R_ECHO: &st}
	case P_REG:
		p := ProducerRegisterMessage{}
		p.fromByte(streamMessage[1:])
		return &Message{P_REG: &p}
	case R_P_REG:
		var st = streamMessage[1]
		return &Message{R_P_REG: &st}
	case C_REG:
		p := ConsumerRegisterMessage{}
		p.fromByte(streamMessage[1:])
		return &Message{C_REG: &p}
	case R_C_REG:
		var st = streamMessage[1]
		return &Message{R_C_REG: &st}
	case PCM:
		return &Message{PCM: streamMessage[1:]}
	case R_PCM:
		var st = streamMessage[1]
		return &Message{R_PCM: &st}
	default:
		return nil
	}
}

func readMessageFromStream(streamRW *bufio.ReadWriter) (*Message, error) {
	data, err := readFromStream(streamRW)
	if err != nil {
		return nil, err
	}
	return parseMessage(data), nil
}

func writeDataToStreamWithType(streamRW *bufio.ReadWriter, mType byte, data string) error {
	var err error
	// Write length
	err = streamRW.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	// Write type
	err = streamRW.WriteByte(mType)
	if err != nil {
		return err
	}
	// Write data
	_, err = streamRW.WriteString(data)
	if err != nil {
		return err
	}
	err = streamRW.Flush()
	if err != nil {
		return err
	}

	return nil
}

func writeMessageToStream(streamRW *bufio.ReadWriter, message Message) error {
	if message.ECHO != nil {
		if err := writeDataToStreamWithType(streamRW, ECHO, *message.ECHO); err != nil {
			return err
		}
	} else if message.R_ECHO != nil {
		if err := writeDataToStreamWithType(streamRW, R_ECHO, *message.R_ECHO); err != nil {
			return err
		}
	}
	if message.P_REG != nil {
		data := string(message.P_REG.toByte())
		if err := writeDataToStreamWithType(streamRW, P_REG, data); err != nil {
			return err
		}
	}
	if message.R_P_REG != nil {
		data := fmt.Sprintf("%d", *message.R_P_REG)
		if err := writeDataToStreamWithType(streamRW, R_P_REG, data); err != nil {
			return err
		}
	}
	if message.C_REG != nil {
		data := string(message.C_REG.toByte())
		if err := writeDataToStreamWithType(streamRW, C_REG, data); err != nil {
			return err
		}
	}
	if message.R_C_REG != nil {
		data := fmt.Sprintf("%d", *message.R_C_REG)
		if err := writeDataToStreamWithType(streamRW, R_C_REG, data); err != nil {
			return err
		}
	}
	if message.PCM != nil {
		if err := writeDataToStreamWithType(streamRW, PCM, string(message.PCM)); err != nil {
			return err
		}
	}
	if message.R_PCM != nil {
		data := fmt.Sprintf("%d", *message.R_PCM)
		if err := writeDataToStreamWithType(streamRW, R_PCM, data); err != nil {
			return err
		}
	}
	return nil
}
