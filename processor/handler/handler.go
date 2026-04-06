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
	for {
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				continue
			}
			if msg.Topic == "source" {
				if err := h.readSource(common.NewFromBytes(msg.Value, false)); err != nil {
					slog.Error("error reading source message", slog.Any("err", err))
					continue
				}
				sess.MarkMessage(msg, "")
				continue
			}
			slog.Error("no handler found for topic", msg.Topic)
			timer.Reset(h.maxIdealTime)
		case <-timer.C:
			close(h.exitCh)
			return nil
		}
	}
}

func (h consumerGroupHandler) readSource(msg *common.CSV) error {
	h.batcher.AddMessage(msg)
	return nil
}
