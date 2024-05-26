package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"kafka-go-getting-started/internal/cache"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	cache := cache.NewEventCache(5*time.Minute, 10*time.Minute)
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "sentiments-group",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	topic := "test-confluent-topic"
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %v", err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n", *ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
			var event cloudevents.Event
			if err := event.UnmarshalJSON(ev.Value); err != nil {
				log.Printf("Failed to unmarshal event: %v", err)
				continue
			}

			handleEvent(event, cache)
		}
	}
}

func handleEvent(event cloudevents.Event, cache *cache.EventCache) {
	eventType := event.Type()
	switch eventType {
	case "ws.text.created":
		// receive front the web
		fmt.Printf("received: %s\n", eventType)
		key := createKey(event)
		// check if key already exists
		result, exists := cache.Get(key)
		if !exists {
			cache.Set(key, event)
			fmt.Printf("Stored event in cache: %s\n", event)

		} else {
			fmt.Printf("the event all ready exists %s\n", result)
		}

	case "analyzer.text.analyzed":
		// consumer from analizer
		fmt.Printf("received: %s\n", eventType)
	}
}

func createKey(event cloudevents.Event) string {
	k := md5.New()
	k.Write(event.Data())
	key := hex.EncodeToString(k.Sum(nil))
	fmt.Println("hashing MD5\n", key)
	return key
}
