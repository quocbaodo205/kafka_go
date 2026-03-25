package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println(os.Args)
	if os.Args[1] == "server" {
		var broker = Broker{}
		err := broker.startBrokerServer()
		if err != nil {
			fmt.Printf("Error starting broker: %v\n", err.Error())
		}
	} else {
		clientConnectTCPAndEcho(10000)
	}
}

func writeEchoToStream(stream_rw *bufio.ReadWriter, data string) error {
	var err error
	err = stream_rw.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	err = stream_rw.WriteByte(ECHO)
	if err != nil {
		return err
	}
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

func clientConnectTCPAndEcho(port int) {
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", port))
	fmt.Printf("Connected to server at port %v\n", port)
	// Read input from stdin and write to stream.
	rd := bufio.NewReader(os.Stdin)
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	line, err := rd.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return
		} else {
			// Probably panic here
		}
	}
	fmt.Printf("Sent to server: %s\n", line)
	writeEchoToStream(stream_rw, strings.Trim(line, "\n"))

	// Try to read back from the stream
	header, err := stream_rw.ReadByte()
	if header == 0 || err != nil {
		return
	}
	data, _ := stream_rw.Peek(int(header)) // Read exactly n bytes
	fmt.Printf("Receive message from server: %s\n", data)
	stream_rw.Discard(int(header)) // Throw n bytes away
}
