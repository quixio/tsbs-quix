package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

var (
	kafkaBroker string
	kafkaTopic  string
)

func init() {
	flag.StringVar(&kafkaBroker, "broker", "localhost:9092", "Kafka bootstrap broker")
	flag.StringVar(&kafkaTopic, "topic", "tsbs_data", "Kafka topic to produce to")
}

func main() {
	flag.Parse()

	// Set up Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Start reading from stdin
	count := 0
	flushAt := 10_000
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Bytes()
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          append([]byte(nil), line...), // copy line to avoid reuse
		}, nil)
		if err != nil {
			log.Printf("Failed to produce message: %v", err)
		}
        count++
        if count == flushAt {
            count = 0
            producer.Flush(15_000)
        }
	}

	// Check for read error
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading stdin: %v", err)
	}

	// Wait for all messages to be delivered
	producer.Flush(15_000)
	fmt.Println("All messages flushed.")
}