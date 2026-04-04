// dispatcher reads quality_alerts and fans out to:
//   - Redis sorted set  "dfo:alerts:by_severity"   (score = severity numeric)
//   - Redis hash        "dfo:alerts:latest:<source>" (latest alert per source)
//   - Redis pub/sub     "dfo:alerts:stream"          (real-time fanout)
//   - Snowflake         (stub — logs to file until Snowflake is configured)
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

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const consumerGroup = "dfo-dispatcher"

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
	topicAlerts := cfg.Kafka.Topics["quality_alerts"]

	// ── Kafka reader ───────────────────────────────────────────────────────────
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topicAlerts,
		GroupID:        consumerGroup,
		MinBytes:       1e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	// ── Redis client ───────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.Redis.Addr,
		DB:   cfg.Redis.DB,
	})
	alertTTL := time.Duration(cfg.Redis.AlertTTL) * time.Second

	// ── Snowflake stub ─────────────────────────────────────────────────────────
	// Replace this with the real gosnowflake driver once Snowflake is configured.
	sfLog, err := os.OpenFile("snowflake_alerts.jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Error("cannot open snowflake stub file", "err", err)
		os.Exit(1)
	}
	defer sfLog.Close()
	log.Warn("Snowflake not yet configured — writing alerts to snowflake_alerts.jsonl stub")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Verify Redis connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis ping failed", "addr", cfg.Redis.Addr, "err", err)
		os.Exit(1)
	}
	log.Info("dispatcher started", "topic", topicAlerts, "group", consumerGroup,
		"redis", cfg.Redis.Addr)

	var dispatched int

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Error("read error", "err", err)
			continue
		}

		var alert events.QualityAlert
		if err := json.Unmarshal(msg.Value, &alert); err != nil {
			log.Warn("skipping malformed alert", "err", err)
			continue
		}

		payload := string(msg.Value)
		score := events.SeverityScore(alert.Severity)

		// ── 1. Sorted set — ranked by severity for dashboards ─────────────────
		rdb.ZAdd(ctx, "dfo:alerts:by_severity", redis.Z{
			Score:  score,
			Member: alert.AlertID + "|" + payload, // unique member
		})
		// Trim to last 10 000 alerts
		rdb.ZRemRangeByRank(ctx, "dfo:alerts:by_severity", 0, -10_001)

		// ── 2. Hash — latest alert per source (for quick Grafana lookups) ─────
		latestKey := fmt.Sprintf("dfo:alerts:latest:%s", alert.Source)
		rdb.Set(ctx, latestKey, payload, alertTTL)

		// ── 3. Counter — alert counts per source:type (for rate metrics) ──────
		countKey := fmt.Sprintf("dfo:alert_count:%s:%s", alert.Source, alert.AlertType)
		rdb.Incr(ctx, countKey)
		rdb.Expire(ctx, countKey, alertTTL)

		// ── 4. Pub/Sub — real-time fanout to Grafana Live / websocket clients ─
		rdb.Publish(ctx, "dfo:alerts:stream", payload)

		// ── 5. Snowflake stub — append JSON line ──────────────────────────────
		fmt.Fprintln(sfLog, payload)

		dispatched++
		if dispatched%100 == 0 {
			log.Info("dispatched", "count", dispatched,
				"last_source", alert.Source,
				"last_severity", alert.Severity)
		}
	}

	log.Info("shutting down dispatcher", "total_dispatched", dispatched)
	reader.Close()
	rdb.Close()
}
