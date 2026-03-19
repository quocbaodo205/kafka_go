package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	fmt.Println(os.Args)
	if os.Args[1] == "server" {
		// startServer()
		spawnServer()
	} else {
		clientConnect(os.Args[2])
	}
}

func spawnServer() {
	var wg sync.WaitGroup
	wg.Go(func() { startServer("10001") })
	wg.Go(func() { startServer("10002") })
	wg.Wait()
}

func startServer(port string) {
	ln, _ := net.Listen("tcp", fmt.Sprintf(":%s", port))
	conn, _ := ln.Accept() // Block until can
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		// Read
		header, _ := stream_rw.ReadByte()      // Block
		data, _ := stream_rw.Peek(int(header)) // Block
		fmt.Printf("Data from client: %s\n", data)
		if strings.Trim(string(data), "\n ") == "bye" {
			break
		}
		stream_rw.Discard(int(header))

		time.Sleep(2000 * time.Millisecond)

		// Write
		stream_rw.WriteByte(byte(len(data)))
		newData := fmt.Sprintf("Received from client: %s", string(data))
		stream_rw.WriteString(newData)
		stream_rw.Flush()
	}

	conn.Close()
}

func clientConnect(port string) {
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%s", port))
	for {
		// Read from stdin in a line
		rd := bufio.NewReader(os.Stdin)
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		fmt.Printf("Send to server: %s\n", line)
		// Write to server
		stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		// [0 1 2 3 4 .. 1024]
		stream_rw.WriteByte(byte(len(line)))
		stream_rw.WriteString(line)
		stream_rw.Flush()
		if strings.Trim(line, "\n ") == "bye" {
			break
		}

		// Read
		header, _ := stream_rw.ReadByte()
		data, _ := stream_rw.Peek(int(header))
		fmt.Printf("Data from server: %s\n", data)
		stream_rw.Discard(int(header))
	}

	conn.Close()
}
