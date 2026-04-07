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

	sorter := sort.NewSorter(runID, batchSize)

	subscriber := &Subscriber{}

	csvBatcher := batcher.NewBatcher(batchSize, flushInterval, runID)
	go csvBatcher.Start(ctx)

	if err := subscriber.Subscribe(ctx, csvBatcher, maxEvents, inactivityTimeout); err != nil && !errors.Is(err, errListenerExited) {
		panic(err)
	}
	cancel()

	ctx, cancel = context.WithCancel(ctx)
	if err := sorter.IndividualFileSort(); err != nil {
		panic(err)
	}

	if err := sorter.ExternalSort(producer); err != nil {
		panic(err)
	}
}

// Subscribe consumes messages from the "source" topic and sends them to csvBatcher.
// It reconnects when the consumer is disconnected and stops when ctx is canceled.
//
// maxEventCount is the maximum number of events the handler should process before
// signaling completion.
//
// maxIdealTime is the target processing window for a batch; the handler may stop
// earlier or later depending on runtime conditions.
//
// It returns errListenerExited when consumption ends due to context cancellation.
// Other errors are returned as-is.
func (s *Subscriber) Subscribe(ctx context.Context, csvBatcher *batcher.Batcher, maxEventCount int64, inactivityTimeout time.Duration) error {
	s.getConnection(ctx)

	// Get the list of topics to subscribe
	topics := []string{"source"}

	for {
		closeCh := make(chan struct{})
		eventHandler, err := handler.NewConsumerGroupHandler(csvBatcher, maxEventCount, inactivityTimeout, closeCh)
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
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return errListenerExited
		}
		slog.Debug("consumer handler exited", slog.Any("ctxErr", ctx.Err()))
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
		consumer, err = sarama.NewConsumerGroup(s.brokers, "processor", conf)
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
