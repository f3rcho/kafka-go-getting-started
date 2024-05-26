package kafkapkg

import (
	"context"
	"encoding/json"
	"log"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer(brokers string, topic string) (*KafkaProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers, "acks": "all"})
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		producer: producer,
		topic:    topic,
	}, nil
}

func (p *KafkaProducer) SendEvent(ctx context.Context, eventType string, data interface{}) error {
	// cloud event
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("master sentiments")
	event.SetType(eventType)
	event.SetData(cloudevents.ApplicationJSON, data)

	ttl := time.Now().Add(60 * time.Second).Format(time.RFC3339)
	event.SetExtension("ttl", ttl)

	eventData, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal event: %v", err)
		return err
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          eventData,
	}

	err = p.producer.Produce(message, nil)
	if err != nil {
		log.Printf("Failed to produce message: %v", err)
		return err
	}

	// wait for message delivery
	e := <-p.producer.Events()
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		log.Printf("delivery failed: %v", m.TopicPartition.Error)
		return m.TopicPartition.Error
	} else {
		log.Printf("Delivered message to %v\n", m.TopicPartition)
	}

	return nil
}

func (p *KafkaProducer) Close() {
	p.producer.Flush(15 * 1000)
	p.producer.Close()
}
