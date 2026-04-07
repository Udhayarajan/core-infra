package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"core-infra/processor/batcher"
	"core-infra/processor/handler"
	"core-infra/processor/sort"

	"github.com/IBM/sarama"
)

const fiftyMillion = 50_000_000

var (
	errListenerExited = errors.New("listener exited")
	batchSize         int64 // in bytes
	flushInterval     time.Duration
	maxEvents         int64
	inactivityTimeout time.Duration
)

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
	})))
	flag.Int64Var(&batchSize, "batch", 10_000_000, "size of messages to batch before writing to disk")
	flag.Int64Var(&maxEvents, "max", 2*fiftyMillion, "maximum number of events the listener should process before exiting")
	flag.DurationVar(&inactivityTimeout, "timeout", 2*time.Minute, "duration of inactivity after which the listener should exit")
	flag.DurationVar(&flushInterval, "flush", 10*time.Second, "duration after which the batcher should flush messages to disk")
	flag.Parse()
}

func getUniqueRunID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func main() {

	producer, err := NewAsyncProducer()
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	runID := getUniqueRunID()
	rootPath := filepath.Join("csv", runID)

	subscriber := &Subscriber{}

	csvBatcher := batcher.NewBatcher(maxEvents, batchSize, flushInterval, rootPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan struct{}, 1)
	go func() {
		<-ch
		cancel()
	}()
	go csvBatcher.Start(ctx, ch)

	if err := subscriber.Subscribe(ctx, csvBatcher, inactivityTimeout); err != nil && !errors.Is(err, errListenerExited) {
		panic(err)
	}
	cancel()

	slog.Info("done individual files, doing external sort, this will take some time")
	if err := sort.ExternalSort(rootPath, producer); err != nil {
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
func (s *Subscriber) Subscribe(ctx context.Context, csvBatcher *batcher.Batcher, inactivityTimeout time.Duration) error {
	s.getConnection(ctx)

	// Get the list of topics to subscribe
	topics := []string{"source"}

	for {
		closeCh := make(chan struct{})
		eventHandler, err := handler.NewConsumerGroupHandler(csvBatcher, inactivityTimeout, closeCh)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			<-closeCh
			cancel()
		}()
		slog.Info("starting processor handler")
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

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	client, err := sarama.NewAsyncProducer([]string{broker}, conf)
	if err != nil {
		return nil, err
	}

	return client, nil
}
