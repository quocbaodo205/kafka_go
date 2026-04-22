package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	fmt.Println(os.Args)
	if os.Args[1] == "server" {
		var broker = Broker{}
		broker.init()
		err := broker.startBrokerServer()
		if err != nil {
			fmt.Printf("Error starting broker: %v\n", err.Error())
		}
	} else if os.Args[1] == "producer" {
		fmt.Println("Trying to start producer processes")
		port, err := strconv.ParseInt(os.Args[2], 10, 16)
		if err != nil {
			panic(err)
		}
		topicID, err := strconv.ParseInt(os.Args[3], 10, 16)
		if err != nil {
			panic(err)
		}
		producer := Producer{
			port:    uint16(port),
			topicID: uint16(topicID),
		}
		// producer.startProducerServer()
		producer.startAndSimulateProducerServer()
	} else if os.Args[1] == "consumer" {
		fmt.Println("Trying to start consumer processes")
		port, err := strconv.ParseInt(os.Args[2], 10, 16)
		if err != nil {
			panic(err)
		}
		topicID, err := strconv.ParseInt(os.Args[3], 10, 16)
		if err != nil {
			panic(err)
		}
		groupID, err := strconv.ParseInt(os.Args[4], 10, 16)
		if err != nil {
			panic(err)
		}
		consumer := Consumer{
			port:    uint16(port),
			topicID: uint16(topicID),
			groupID: uint16(groupID),
		}
		consumer.startConsumerServer()
	}
}
