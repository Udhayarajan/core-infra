package main

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"core-infra/validator/handler"

	"github.com/IBM/sarama"
)

func main() {
	subscriber := &Subscriber{}
	if err := subscriber.Subscribe(context.Background()); err != nil {
		panic(err)
	}
}

func (s *Subscriber) Subscribe(ctx context.Context) error {
	s.getConnection(ctx)

	// Get the list of topics to subscribe
	topics := []string{"id", "name", "continent"}

	for {
		eventHandler := handler.NewConsumerGroupHandler()
		slog.Info("starting consumer handler")
		if err := s.consumer.Consume(ctx, topics, eventHandler); err != nil {
			if errors.Is(err, sarama.ErrNotConnected) {
				s.getConnection(ctx)
				continue
			}

			slog.ErrorContext(ctx, "unable to consume", slog.Any("err", err))
		}
	}
}

type Subscriber struct {
	consumer sarama.ConsumerGroup
	brokers  []string
	config   *sarama.Config
	// connecting indicates whether a connection attempt is in progress
	connecting atomic.Bool
}

func (s *Subscriber) getConnection(ctx context.Context) {
	if !s.connecting.CompareAndSwap(false, true) {
		return
	}

	s.brokers = []string{"localhost:9092"}
	var (
		consumer sarama.ConsumerGroup
		err      error
		conf     = sarama.NewConfig()
	)

	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.Return.Errors = true
	conf.Version = sarama.V3_9_0_0

	for consumer == nil {
		consumer, err = sarama.NewConsumerGroup(s.brokers, "validator", conf)
		if err != nil {
			time.Sleep(5 * time.Second) // Wait before retrying
			continue
		}
	}
	s.consumer = consumer
	s.config = conf
	s.connecting.Swap(false)
	slog.DebugContext(ctx, "consumer connected")
}
