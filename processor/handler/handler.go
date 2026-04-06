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
	batcher       *batcher.Batcher
	maxEventCount int
	maxIdealTime  time.Duration
	exitCh        chan struct{}
}

func NewConsumerGroupHandler(batcher *batcher.Batcher, maxEvent int, maxIdealTime time.Duration, ch chan struct{}) (sarama.ConsumerGroupHandler, error) {
	if maxIdealTime <= 0 && maxEvent <= 0 {
		return nil, fmt.Errorf("either maxIdealTime or maxEvent must be greater than 0")
	}
	batcherLimit, batcherTickerDuration := batcher.Limit()
	if maxIdealTime > 0 && batcherTickerDuration.Nanoseconds() > maxIdealTime.Nanoseconds() {
		return nil, fmt.Errorf("ideal wait time is too shorter than batch ticker interval")
	}

	if maxEvent > 0 && batcherLimit > maxEvent {
		return nil, fmt.Errorf("max event count is too shorter than batch limit")
	}

	return &consumerGroupHandler{
		batcher:       batcher,
		maxEventCount: maxEvent,
		maxIdealTime:  maxIdealTime,
		exitCh:        ch,
	}, nil
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	timer := time.NewTimer(h.maxIdealTime)
	defer timer.Stop()
	eventCount := 0
	for {
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				continue
			}
			timer.Reset(h.maxIdealTime)
			eventCount++
			if msg.Topic == "source" {
				h.addMessage(common.NewFromBytes(msg.Value, false))
				sess.MarkMessage(msg, "")
				if eventCount >= h.maxEventCount {
					close(h.exitCh)
					return nil
				}
				continue
			}
			slog.Error("no handler found for topic", msg.Topic)
		case <-timer.C:
			slog.Info("no message received within ideal time, exiting consumer handler", slog.Any("max_ideal_time", h.maxIdealTime))
			close(h.exitCh)
			return nil
		}
	}
}

func (h consumerGroupHandler) addMessage(msg *common.CSV) {
	h.batcher.AddMessage(msg)
}
