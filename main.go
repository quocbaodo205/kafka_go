package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

func main() {
	fmt.Println(os.Args)
	if os.Args[1] == "server" {
		var broker = Broker{}
		err := broker.startBrokerServer()
		if err != nil {
			fmt.Printf("Error starting broker: %v\n", err.Error())
		}
	} else if os.Args[1] == "producer" {
		fmt.Println("Trying to start producer processes")
		port, err := strconv.ParseInt(os.Args[2], 10, 32)
		if err != nil {
			panic(err)
		}
		producer := Producer{}
		producer.startProducerServer(int16(port))
	} else {
		clientConnectTCPAndEcho(10000)
	}
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
	message := Message{
		ECHO: &line,
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

	fmt.Printf("Receive message from server: %s\n", *resp.R_ECHO)
}
