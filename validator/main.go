package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"core-infra/validator/handler"

	"github.com/IBM/sarama"
)

const fiftyMillion = 50_000_000

var (
	maxEvents int64
)

func init() {
	flag.Int64Var(&maxEvents, "max", 2*fiftyMillion, "maximum number of events the listener should process before exiting")
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
	})))
}

func main() {
	subscriber := &Subscriber{}
	if err := subscriber.Subscribe(context.Background()); err != nil {
		panic(err)
	}
	slog.Info("check sorted files under csv/ directory")
}

func (s *Subscriber) Subscribe(ctx context.Context) error {
	s.getConnection(ctx)

	// Get the list of topics to subscribe
	topics := []string{"id", "name", "continent"}

	for {
		ch := make(chan struct{})
		eventHandler := handler.NewConsumerGroupHandler(ch)
		slog.Info("starting validator handler")
		consumeCtx, cancel := context.WithCancel(ctx)
		var timeoutTriggered atomic.Bool
		done := make(chan struct{})
		go func() {
			select {
			case <-ch:
				timeoutTriggered.Store(true)
				cancel()
			case <-done:
			}
		}()
		err := s.consumer.Consume(consumeCtx, topics, eventHandler)
		close(done)
		if err != nil {
			cancel()
			if timeoutTriggered.Load() {
				slog.InfoContext(ctx, "validator exited after inactivity timeout")
				return nil
			}
			if errors.Is(err, sarama.ErrNotConnected) {
				s.getConnection(ctx)
				continue
			}

			slog.ErrorContext(ctx, "unable to consume", slog.Any("err", err))
		}
		cancel()
		if timeoutTriggered.Load() {
			slog.InfoContext(ctx, "validator exited after inactivity timeout")
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
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

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	s.brokers = []string{broker}
	var (
		consumer sarama.ConsumerGroup
		err      error
		conf     = sarama.NewConfig()
	)

	conf.Consumer.Offsets.Initial = sarama.OffsetNewest
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
