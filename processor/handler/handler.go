package handler

import (
	"fmt"
	"log/slog"
	"time"

	"core-infra/common"
	"core-infra/processor/batcher"

	"github.com/IBM/sarama"
)

type consumerGroupHandler struct {
	batcher                   *batcher.Batcher
	inactivityTimeoutDuration time.Duration
	exitCh                    chan struct{}
}

func NewConsumerGroupHandler(batcher *batcher.Batcher, inactivityTimeout time.Duration, ch chan struct{}) (sarama.ConsumerGroupHandler, error) {
	if inactivityTimeout <= 0 {
		return nil, fmt.Errorf("inactivity timeout must be greater than zero")
	}
	_, batcherTickerDuration := batcher.Limit()
	if batcherTickerDuration.Nanoseconds() > inactivityTimeout.Nanoseconds() {
		return nil, fmt.Errorf("ideal wait time is too shorter than batch ticker interval")
	}

	return &consumerGroupHandler{
		batcher:                   batcher,
		inactivityTimeoutDuration: inactivityTimeout,
		exitCh:                    ch,
	}, nil
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	timer := time.NewTimer(h.inactivityTimeoutDuration)
	defer timer.Stop()
	eventCount := int64(0)
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if msg == nil {
				continue
			}
			if msg.Topic == "source" {
				timer.Reset(h.inactivityTimeoutDuration)
				h.addMessage(common.NewFromBytes(msg.Value, false))
				sess.MarkMessage(msg, "")
				eventCount++
				continue
			}
			slog.Error("no handler found for topic", slog.String("topic", msg.Topic))
		case <-timer.C:
			slog.Info("no message received within ideal time, exiting consumer handler", slog.Any("max_ideal_time", h.inactivityTimeoutDuration))
			close(h.exitCh)
			return nil
		}
	}
}

func (h consumerGroupHandler) addMessage(msg *common.CSV) {
	h.batcher.AddMessage(msg)
}
