package main

import (
	"bufio"
	"fmt"
)

const (
	ECHO  = 1
	P_REG = 2
	// Response
	R_ECHO  = 101
	R_P_REG = 102
)

type Message struct {
	ECHO  *string
	P_REG *string
	// Response
	R_ECHO  *string
	R_P_REG *byte
}

// Message format:
// - stream[0]: size
// -> stream[1:]: []byte
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

func parseMessage(stream_message []byte) *Message {
	switch stream_message[0] {
	case ECHO:
		var st = string(stream_message[1:])
		return &Message{ECHO: &st}
	case R_ECHO:
		var st = string(stream_message[1:])
		return &Message{R_ECHO: &st}
	case P_REG:
		var st = string(stream_message[1:])
		return &Message{P_REG: &st}
	case R_P_REG:
		var st = stream_message[1]
		return &Message{R_P_REG: &st}
	default:
		return nil
	}
}

func readMessageFromStream(stream_rw *bufio.ReadWriter) (*Message, error) {
	data, err := readFromStream(stream_rw)
	if err != nil {
		return nil, err
	}
	return parseMessage(data), nil
}

func writeDataToStreamWithType(stream_rw *bufio.ReadWriter, mtype byte, data string) error {
	var err error
	// Write length
	err = stream_rw.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	// Write type
	err = stream_rw.WriteByte(mtype)
	if err != nil {
		return err
	}
	// Write data
	_, err = stream_rw.WriteString(data)
	if err != nil {
		return err
	}
	err = stream_rw.Flush()
	if err != nil {
		return err
	}

	return nil
}

// [ 7  1  h e l l o o ]
func writeMessageToStream(stream_rw *bufio.ReadWriter, message Message) error {
	if message.ECHO != nil {
		if err := writeDataToStreamWithType(stream_rw, ECHO, *message.ECHO); err != nil {
			return err
		}
	} else if message.R_ECHO != nil {
		if err := writeDataToStreamWithType(stream_rw, R_ECHO, *message.R_ECHO); err != nil {
			return err
		}
	}
	if message.P_REG != nil {
		if err := writeDataToStreamWithType(stream_rw, P_REG, *message.P_REG); err != nil {
			return err
		}
	}
	if message.R_P_REG != nil {
		data := fmt.Sprintf("%d", *message.R_P_REG)
		if err := writeDataToStreamWithType(stream_rw, R_P_REG, data); err != nil {
			return err
		}
	}
	return nil
}
