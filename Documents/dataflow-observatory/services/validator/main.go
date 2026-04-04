// validator reads raw_events, runs DQ checks (schema, null rates, anomalies),
// and publishes QualityAlerts to quality_alerts and hard failures to dead_letter.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dataflow-observatory/services/internal/config"
	"dataflow-observatory/services/internal/events"
	"dataflow-observatory/services/internal/validate"

	"github.com/segmentio/kafka-go"
)

const consumerGroup = "dfo-validators"

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
	topicAlerts := cfg.Kafka.Topics["quality_alerts"]
	topicDL := cfg.Kafka.Topics["dead_letter"]

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topicIn,
		GroupID:        consumerGroup,
		MinBytes:       1e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	alertWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topicAlerts,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
	}
	dlWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topicDL,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
	}

	v := validate.New(
		cfg.Validator.NullRateThreshold,
		cfg.Validator.AnomalyZScore,
		cfg.Validator.WindowSeconds,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var totalAlerts, totalEvents int

	log.Info("validator started", "topic", topicIn, "group", consumerGroup,
		"null_threshold", cfg.Validator.NullRateThreshold,
		"z_threshold", cfg.Validator.AnomalyZScore)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Error("read error", "err", err)
			continue
		}
		totalEvents++

		// ── parse ──────────────────────────────────────────────────────────────
		var ev events.RawEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			dl := events.DeadLetterEvent{
				OriginalPayload: string(msg.Value),
				Reason:          fmt.Sprintf("json parse: %v", err),
				Timestamp:       time.Now().UTC().Format(time.RFC3339),
				Partition:       int(msg.Partition),
				Offset:          msg.Offset,
			}
			publish(ctx, dlWriter, msg.Key, dl, log)
			continue
		}

		// ── run DQ checks ──────────────────────────────────────────────────────
		alerts := v.Check(&ev)
		totalAlerts += len(alerts)

		for _, alert := range alerts {
			publish(ctx, alertWriter, []byte(alert.Source), alert, log)
			log.Info("alert",
				"type", alert.AlertType,
				"source", alert.Source,
				"field", alert.Field,
				"severity", alert.Severity,
				"msg", alert.Message,
			)
		}

		if totalEvents%1000 == 0 {
			log.Info("progress",
				"events_processed", totalEvents,
				"alerts_generated", totalAlerts,
				"alert_rate", fmt.Sprintf("%.2f%%", float64(totalAlerts)/float64(totalEvents)*100),
			)
		}
	}

	log.Info("shutting down validator",
		"events_processed", totalEvents,
		"alerts_generated", totalAlerts,
	)
	reader.Close()
	alertWriter.Close()
	dlWriter.Close()
}

func publish(ctx context.Context, w *kafka.Writer, key []byte, v any, log *slog.Logger) {
	b, err := json.Marshal(v)
	if err != nil {
		log.Error("marshal error", "err", err)
		return
	}
	if err := w.WriteMessages(ctx, kafka.Message{Key: key, Value: b}); err != nil && ctx.Err() == nil {
		log.Error("write error", "topic", w.Topic, "err", err)
	}
}
