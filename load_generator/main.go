package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Message struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "input-topic")
	numWorkers := getEnvInt("NUM_WORKERS", 48)
	batchSize := getEnvInt("BATCH_SIZE", 1000)

	totalMessages := 0
	if len(os.Args) > 1 {
		fmt.Sscanf(os.Args[1], "%d", &totalMessages)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		BatchSize:    batchSize,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer writer.Close()

	var totalSent int64
	var wg sync.WaitGroup

	if totalMessages > 0 {
		fmt.Printf("Starting load generator: %d workers (batch: %d) -> %s topic | total: %d messages\n", numWorkers, batchSize, topic, totalMessages)
	} else {
		fmt.Printf("Starting load generator: %d workers (batch: %d) -> %s topic | infinite mode\n", numWorkers, batchSize, topic)
		fmt.Printf("Tip: pass a number as argument to limit messages (e.g., go run main.go 1000000)\n")
	}
	fmt.Printf("Press Ctrl+C to stop\n\n")

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if totalMessages > 0 && atomic.LoadInt64(&totalSent) >= int64(totalMessages) {
					return
				}

				msg := Message{
					ID:        uuid.New().String(),
					Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
					Data:      randomData(),
				}

				value, _ := json.Marshal(msg)
				err := writer.WriteMessages(ctx, kafka.Message{
					Key:   []byte(msg.ID),
					Value: value,
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "write error: %v\n", err)
					continue
				}

				atomic.AddInt64(&totalSent, 1)
			}
		}(i)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	var prevCount int64
	startTime := time.Now()

	for {
		select {
		case <-done:
			duration := time.Since(startTime).Seconds()
			total := atomic.LoadInt64(&totalSent)
			avgRate := float64(total) / duration
			fmt.Printf("\n\n%s✓ Load completed!%s\n", "\033[32m", "\033[0m")
			fmt.Printf("  Duration:   %.2fs\n", duration)
			fmt.Printf("  Messages:   %d\n", total)
			fmt.Printf("  Avg rate:   %.0f msg/s\n", avgRate)
			fmt.Printf("  Msg/batch:  %.0f\n", avgRate*duration/float64(numWorkers))
			return
		case <-ctx.Done():
			wg.Wait()
			duration := time.Since(startTime).Seconds()
			total := atomic.LoadInt64(&totalSent)
			avgRate := float64(total) / duration
			fmt.Printf("\n\n%s✗ Stopped!%s\n", "\033[33m", "\033[0m")
			fmt.Printf("  Duration:   %.2fs\n", duration)
			fmt.Printf("  Messages:   %d\n", total)
			fmt.Printf("  Avg rate:   %.0f msg/s\n", avgRate)
			return
		case <-ticker.C:
			current := atomic.LoadInt64(&totalSent)
			rate := current - prevCount
			prevCount = current
			elapsed := time.Since(startTime).Seconds()
			avgRate := float64(current) / elapsed
			fmt.Printf("\r[throughput] %d msg/s | total: %d | elapsed: %.1fs | avg: %.0f msg/s", rate, current, elapsed, avgRate)
		}
	}
}

func randomData() string {
	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(rand.Intn(26) + 'a')
	}
	return string(data)
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		var n int
		fmt.Sscanf(val, "%d", &n)
		if n > 0 {
			return n
		}
	}
	return fallback
}
