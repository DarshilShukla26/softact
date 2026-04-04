// ingestor reads raw_events, routes malformed JSON to dead_letter,
// and prints per-source throughput stats every 5 seconds.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"dataflow-observatory/services/internal/config"
	"dataflow-observatory/services/internal/events"

	"github.com/segmentio/kafka-go"
)

const consumerGroup = "dfo-ingestor"

func main() {
	cfgPath := flag.String("config", "../config.yaml", "path to config.yaml")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	brokers := cfg.Kafka.Brokers
	topicIn := cfg.Kafka.Topics["raw_events"]
	topicDL := cfg.Kafka.Topics["dead_letter"]

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topicIn,
		GroupID:        consumerGroup,
		MinBytes:       1e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	dlWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topicDL,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// per-source counters
	var mu sync.Mutex
	counts := map[string]int{}
	dlCount := 0
	lastLog := time.Now()

	log.Info("ingestor started", "topic", topicIn, "group", consumerGroup)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // graceful shutdown
			}
			log.Error("read error", "err", err)
			continue
		}

		var ev events.RawEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			// malformed JSON → dead letter
			dl := events.DeadLetterEvent{
				OriginalPayload: string(msg.Value),
				Reason:          fmt.Sprintf("json parse error: %v", err),
				Timestamp:       time.Now().UTC().Format(time.RFC3339),
				Partition:       int(msg.Partition),
				Offset:          msg.Offset,
			}
			dlBytes, _ := json.Marshal(dl)
			if werr := dlWriter.WriteMessages(ctx, kafka.Message{
				Key:   msg.Key,
				Value: dlBytes,
			}); werr != nil {
				log.Warn("dead-letter write failed", "err", werr)
			}
			mu.Lock()
			dlCount++
			mu.Unlock()
			continue
		}

		mu.Lock()
		counts[ev.Source]++
		mu.Unlock()

		// print stats every 5 s
		if time.Since(lastLog) >= 5*time.Second {
			mu.Lock()
			snapshot := make(map[string]int, len(counts))
			for k, v := range counts {
				snapshot[k] = v
				counts[k] = 0
			}
			dl := dlCount
			dlCount = 0
			mu.Unlock()

			elapsed := time.Since(lastLog).Seconds()
			for src, n := range snapshot {
				log.Info("throughput",
					"source", src,
					"events", n,
					"rps", fmt.Sprintf("%.0f", float64(n)/elapsed),
				)
			}
			if dl > 0 {
				log.Warn("dead-lettered", "count", dl)
			}
			lastLog = time.Now()
		}
	}

	log.Info("shutting down ingestor")
	if err := reader.Close(); err != nil {
		log.Error("reader close", "err", err)
	}
	if err := dlWriter.Close(); err != nil {
		log.Error("writer close", "err", err)
	}
}
