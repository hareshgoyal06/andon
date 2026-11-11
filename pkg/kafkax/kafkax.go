// Package kafkax wraps segmentio/kafka-go with the small set of helpers the
// services need: topic bootstrap, JSON producers, and consumer-group readers.
package kafkax

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Topics used by the system.
const (
	TopicEvents   = "station.events"   // gateway -> processor/api
	TopicCommands = "station.commands" // api -> gateway
	TopicAlerts   = "station.alerts"   // processor -> api
	TopicOEE      = "station.oee"      // processor -> api
)

// Brokers parses a comma-separated broker list.
func Brokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// EnsureTopics creates topics if they do not exist. It retries until the
// broker is reachable or ctx is cancelled, since Kafka often starts after us.
func EnsureTopics(ctx context.Context, log *slog.Logger, brokers []string, partitions int, topics ...string) error {
	backoff := time.Second
	for {
		err := ensureTopics(ctx, brokers, partitions, topics)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Warn("kafka not ready", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

func ensureTopics(ctx context.Context, brokers []string, partitions int, topics []string) error {
	d := kafka.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	ctrl, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()

	cfgs := make([]kafka.TopicConfig, 0, len(topics))
	for _, t := range topics {
		cfgs = append(cfgs, kafka.TopicConfig{Topic: t, NumPartitions: partitions, ReplicationFactor: 1})
	}
	if err := cc.CreateTopics(cfgs...); err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	return nil
}

// Producer publishes JSON messages keyed by a string.
type Producer struct {
	w *kafka.Writer
}

// NewProducer creates a producer for topic.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // same key -> same partition -> per-station ordering
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}}
}

// Publish marshals v and writes it with key.
func (p *Producer) Publish(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: b})
}

// Close flushes and closes.
func (p *Producer) Close() error { return p.w.Close() }

// ReaderConfig configures a consumer.
type ReaderConfig struct {
	Brokers []string
	Topic   string
	GroupID string
	// FromLatest starts at the newest offset for new groups (used by API fan-out).
	FromLatest bool
}

// NewReader creates a consumer-group reader.
func NewReader(cfg ReaderConfig) *kafka.Reader {
	rc := kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        250 * time.Millisecond,
		CommitInterval: 0, // explicit commits for at-least-once
	}
	if cfg.FromLatest {
		rc.StartOffset = kafka.LastOffset
	} else {
		rc.StartOffset = kafka.FirstOffset
	}
	return kafka.NewReader(rc)
}

// Consume runs fn for each message and commits after success. Returning an
// error from fn stops consumption; the message will be redelivered to the group.
func Consume(ctx context.Context, r *kafka.Reader, fn func(ctx context.Context, m kafka.Message) error) error {
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}
		if err := fn(ctx, m); err != nil {
			return err
		}
		if err := r.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
}
