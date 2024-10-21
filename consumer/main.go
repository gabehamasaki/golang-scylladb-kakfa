package main

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/table"
)

const (
	KafkaServer  = "localhost:9092"
	KafkaTopic   = "reporting"
	KafkaGroupId = "reporting-service"
)

type ReportingDetail struct {
	ID   string
	Name string
	Data string
}

type ReportingDetailData struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

func main() {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "reporting"
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected to the cluster")

	reportingDetailMetadata := table.Metadata{
		Name:    "reporting_detail",
		Columns: []string{"id", "name", "data"},
		PartKey: []string{"id"},
		SortKey: []string{"name"},
	}

	reportingDetailTable := table.New(reportingDetailMetadata)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaServer,
		"group.id":          KafkaGroupId,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	topic := KafkaTopic
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		panic(err)
	}

	result := make(chan string, 10)

	fmt.Printf("Subscribed to topic: %s\n", topic)
	go consumeMessage(c, session, reportingDetailTable, result)

	for {
		select {
		case msg := <-result:
			fmt.Printf("Received message: %s\n", msg)
		}
	}
}

func consumeMessage(c *kafka.Consumer, session gocqlx.Session, reportingDetailTable *table.Table, result chan string) {
	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			result <- fmt.Sprintf("Consumer error: %v (%v)", err, msg)
			continue
		}

		var detailDto ReportingDetailData
		if err = json.Unmarshal(msg.Value, &detailDto); err != nil {
			result <- fmt.Sprintf("Error unmarshalling message: %v", err)
			continue
		}

		detail := ReportingDetail{
			ID:   uuid.New().String(),
			Name: detailDto.Name,
			Data: detailDto.Data,
		}

		q := session.Query(reportingDetailTable.Insert()).BindStruct(&detail)
		result <- fmt.Sprintf("Name: %s, Data: %s", detail.Name, detail.Data)
		if err := q.ExecRelease(); err != nil {
			result <- fmt.Sprintf("Error executing query: %v", err)
		}
	}
}
