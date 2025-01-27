package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/streadway/amqp"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	UserID    string `json:"user_id"`
	IPAddress string `json:"ip_address"`
	Endpoint  string `json:"endpoint"`
}

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Configure Elasticsearch client with HTTPS
	cfg := elasticsearch.Config{
		Addresses: []string{
			"https://localhost:9200", // Use HTTPS instead of HTTP
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting Elasticsearch info: %v", err)
	}
	defer res.Body.Close()
	log.Println("Connected to Elasticsearch:", res.Status())

	// Consume logs
	msgs, err := ch.Consume(
		"logs", // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	for msg := range msgs {
		var logEntry LogEntry
		json.Unmarshal(msg.Body, &logEntry)

		doc, _ := json.Marshal(logEntry)
		res, err := es.Index(
			"logs", // index name
			strings.NewReader(string(doc)),
			es.Index.WithContext(context.Background()),
		)
		if err != nil {
			log.Printf("Error indexing log: %v", err)
		} else {
			defer res.Body.Close()
			fmt.Printf("Indexed log: %s\n", logEntry.Message)
		}
	}
}
