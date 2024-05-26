package main

import (
	"context"
	kafka "kafka-go-getting-started/pkg/kafkapkg"
	"log"
)

func main() {
	producer, err := kafka.NewKafkaProducer("localhost:9092", "test-confluent-topic")
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	defer producer.Close()

	data := map[string]interface{}{
		"id":      "666",
		"message": "the devil comes from who you are",
	}

	eventType := "ws.text.created"

	if err := producer.SendEvent(context.Background(), eventType, data); err != nil {
		log.Fatalf("Failed to send event: %v", err)
	}
}
