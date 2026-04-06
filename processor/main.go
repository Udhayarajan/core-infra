package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"core-infra/processor/batcher"
	"core-infra/processor/handler"
	"core-infra/processor/sort"

	"github.com/IBM/sarama"
)

var errListenerExited = errors.New("listener exited")

func getUniqueRunID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer, err := NewAsyncProducer()
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	runID := getUniqueRunID()
	subscriber := &Subscriber{}
	csvBatcher := batcher.NewBatcher(1000, 20*time.Second, runID)
	go csvBatcher.Start(ctx)
	if err := subscriber.Subscribe(ctx, csvBatcher, -1, 30*time.Second); err != nil && !errors.Is(err, errListenerExited) {
		panic(err)
	}
	cancel()

	ctx, cancel = context.WithCancel(ctx)
	if err := sort.IndividualFileSort(runID); err != nil {
		panic(err)
	}

	if err := sort.ExternalSort(runID, producer); err != nil {
		panic(err)
	}
}

func (s *Subscriber) Subscribe(ctx context.Context, csvBatcher *batcher.Batcher, maxEventCount int32, maxIdealTime time.Duration) error {
	s.getConnection(ctx)

	// Get the list of topics to subscribe
	topics := []string{"source"}

	for {
		closeCh := make(chan struct{})
		eventHandler, err := handler.NewConsumerGroupHandler(csvBatcher, int(maxEventCount), maxIdealTime, closeCh)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			<-closeCh
			cancel()
		}()
		slog.Info("starting consumer handler")
		if err := s.consumer.Consume(ctx, topics, eventHandler); err != nil {
			cancel()
			if errors.Is(err, sarama.ErrNotConnected) {
				s.getConnection(ctx)
				continue
			}

			slog.ErrorContext(ctx, "unable to consume", slog.Any("err", err))
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return errListenerExited
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
		consumer, err = sarama.NewConsumerGroup(s.brokers, "consumer", conf)
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

func NewAsyncProducer() (sarama.AsyncProducer, error) {
	conf := sarama.NewConfig()
	conf.ClientID = "processor"
	conf.Producer.Flush.Messages = 1000
	conf.Producer.Flush.Frequency = 500 * time.Millisecond
	conf.Producer.Compression = sarama.CompressionSnappy
	conf.Producer.RequiredAcks = sarama.NoResponse

	client, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, conf)
	if err != nil {
		return nil, err
	}

	return client, nil
}
