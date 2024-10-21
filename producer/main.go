package main

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	KafkaServer = "localhost:9092"
	Topic       = "reporting"
)

type ReportingDetail struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": KafkaServer})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	topic := Topic

	for i := 0; i <= 1000000; i++ {
		reportingDetail := ReportingDetail{
			Name: "Rick-" + strconv.Itoa(i),
			Data: "You are awesome!",
		}

		value, err := json.Marshal(reportingDetail)
		if err != nil {
			log.Fatalf("Failed to marshal reporting detail: %s", err)
		}

		p.Flush(15 * 1000)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
		}, nil)
		if err != nil {
			log.Fatalf("Failed to produce message: %s", err)
		}
	}

}


