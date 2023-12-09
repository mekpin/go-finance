package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"go-finance/config"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func init() {

}

func main() {
	fmt.Println("kafka consume test")
	// Initialize the Kafka writer
	mechanism := plain.Mechanism{
		Username: config.Common.KafkaUsername,
		Password: config.Common.KafkaPassword,
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	// make a new reader that consumes from mekpin-perlu-belanja, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.Common.KafkaUrl},
		Topic:   "mekpin-perlu-belanja",
		// Partition: 0,
		MaxBytes: 10e6, // 10MB
		Dialer:   dialer,
		GroupID:  "go-finance",
	})

	r.SetOffset(0)

	// Create a context that will be used to control the consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a signal channel to gracefully stop the consumer on termination
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	for {
		fmt.Println("consumer loop jalan")
		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println("error")
			break
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		// ceritanya proses ke DB
		// syalalalala
		// ceritanya selesai proses ke DB
		fmt.Println("cart belanja sudah di catat")
		fmt.Println("---------------------------------------------------------------------------------------")

		// Mark the message as "done" (acknowledge)
		err = r.CommitMessages(ctx, m)
		if err != nil {
			log.Fatal("Error committing message:", err)
			return
		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

	fmt.Println("Consumer started. Press Ctrl+C to stop.")

	// Wait for a termination signal
	select {
	case <-signals:
		fmt.Println("Received termination signal. Stopping consumer.")
		cancel() // Stop the consumer gracefully
		r.Close()
		// Optionally perform cleanup or other tasks before exiting
	}

}
