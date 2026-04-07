package handler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type consumerGroupHandler struct {
	timeout     time.Duration
	timeoutCh   chan struct{}
	timeoutOnce sync.Once
	mu          sync.Mutex
	lastSeen    *time.Time
	csv         *csvWriters
}

func NewConsumerGroupHandler(ch chan struct{}) sarama.ConsumerGroupHandler {
	return &consumerGroupHandler{
		timeout:   20 * time.Second,
		timeoutCh: ch,
	}
}

func (h *consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	csv, err := newCSVWriters("csv", []string{"id", "name", "continent"})
	if err != nil {
		return fmt.Errorf("init csv writers: %w", err)
	}
	h.csv = csv

	go h.watchInactivity(sess.Context())
	return nil
}

func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	if h.csv != nil {
		h.csv.Close()
		h.csv = nil
	}
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if msg == nil {
				continue
			}

			h.mu.Lock()
			h.lastSeen = new(time.Now())
			h.mu.Unlock()

			line := append(msg.Value, '\n')

			if !h.csv.Write(msg.Topic, line) {
				continue
			}

			sess.MarkMessage(msg, "")
		case <-sess.Context().Done():
			return nil
		}
	}
}

func (h *consumerGroupHandler) watchInactivity(ctx context.Context) {
	ticker := time.NewTicker(h.timeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.mu.Lock()
			if h.lastSeen == nil {
				h.mu.Unlock()
				slog.Info("no messages received yet, waiting for activity")
				continue
			}
			idle := time.Since(*h.lastSeen)
			h.mu.Unlock()

			slog.Info("watching inactivity", slog.Time("last_seen", *h.lastSeen), slog.Duration("idle_time", idle))

			if idle >= h.timeout {
				slog.Info("validator inactivity timeout reached, shutting down")
				h.timeoutOnce.Do(func() {
					close(h.timeoutCh)
				})
				return
			}
		}
	}
}
