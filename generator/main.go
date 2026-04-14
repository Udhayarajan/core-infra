package main

import (
	"flag"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"core-infra/generator/data"

	"github.com/IBM/sarama"
)

var (
	totalMessages  int64
	totalGenerated atomic.Int64
	totalErrors    atomic.Int64
	totalSuccess   atomic.Int64
	totalWorkers   int
	debug          bool
	retryCh        = make(chan *sarama.ProducerMessage, 1000)
	droppedCount   atomic.Int64
)

const (
	progressInterval = 5 * time.Second
)

func init() {
	flag.Int64Var(&totalMessages, "max", 50_000_000, "maximum number of messages or event to be generated")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.IntVar(&totalWorkers, "workers", runtime.NumCPU(), "number of concurrent workers to generate data (default: number of CPU cores)")
	flag.Parse()

	logLevel := slog.LevelInfo
	if debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     logLevel,
	})))
}

func main() {
	producer, err := newAsyncProducer()
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	// Distribute totalMessages across workers so the sum equals totalMessages.
	// Use integer division for the base count and distribute the remainder
	// one-by-one to the first `remainder` workers. This ensures exact
	// production even when totalMessages is not divisible by numWorkers.
	if totalMessages < 0 {
		totalMessages = 0
	}
	basePerWorker := int64(0)
	remainder := int64(0)
	if totalWorkers > 0 {
		basePerWorker = totalMessages / int64(totalWorkers)
		remainder = totalMessages % int64(totalWorkers)
	}

	var wg sync.WaitGroup
	slog.Info("Starting producer", slog.Any("total_messages", totalMessages), slog.Any("num_workers", totalWorkers), slog.Any("base_per_worker", basePerWorker), slog.Any("remainder", remainder))

	go retry(producer)
	go handleErrors(producer)
	go func() {
		for range producer.Successes() {
			totalSuccess.Add(1)
		}
	}()

	start := time.Now()
	progressDone := make(chan struct{})
	go progressLogger(progressDone, start)

	for i := 0; i < totalWorkers; i++ {
		// Each worker gets basePerWorker, and the first `remainder` workers
		// receive one extra message to account for the division remainder.
		count := basePerWorker
		if int64(i) < remainder {
			count++
		}
		wg.Add(1)
		go generateDataWorker(&wg, count, producer)
	}

	wg.Wait()
	close(progressDone)
	close(retryCh)

	produced := totalGenerated.Load()
	slog.Info("pipeline complete",
		slog.Any("total", produced),
		slog.Any("errors", totalErrors.Load()),
		slog.Any("success", totalSuccess.Load()),
		slog.String("duration", time.Since(start).String()),
		slog.Any("throughput_msg_per_sec", float64(produced)/time.Since(start).Seconds()),
		slog.Any("dropped_due_to_retry_queue_full", droppedCount.Load()),
	)
}

func handleErrors(producer sarama.AsyncProducer) {
	for err := range producer.Errors() {
		totalErrors.Add(1)

		msg := err.Msg
		retryCount, _ := msg.Metadata.(int)
		retryCount++
		msg.Metadata = retryCount

		if msg.Metadata.(int) <= 3 {
			select {
			case retryCh <- msg:
			default:
				droppedCount.Add(1)
				slog.Warn("retry queue full, dropping message")
			}
		}
	}
}

func retry(producer sarama.AsyncProducer) {
	for msg := range retryCh {
		producer.Input() <- msg
	}
}

func generateDataWorker(wg *sync.WaitGroup, dataPerWorker int64, producer sarama.AsyncProducer) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	gen := data.NewGenerator(rng)

	for j := int64(0); j < dataPerWorker; j++ {
		payload := gen.Generate()
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "source",
			Value: payload,
		}
		totalGenerated.Add(1)
	}
}

func progressLogger(done chan struct{}, start time.Time) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			produced := totalGenerated.Load()
			pct := 0.0
			if totalMessages > 0 {
				pct = (float64(produced) / float64(totalMessages)) * 100
			}
			elapsed := time.Since(start).Seconds()
			rate := 0.0
			if elapsed > 0 {
				rate = float64(produced) / elapsed
			}

			slog.Debug("progress",
				slog.Int64("produced", produced),
				slog.Any("success", totalSuccess.Load()),
				slog.Int64("total", totalMessages),
				slog.Float64("percent", pct),
				slog.Int64("errors", totalErrors.Load()),
				slog.Any("elapsed", time.Since(start).String()),
				slog.Float64("throughput_msg_per_sec", rate),
				slog.Any("dropped_due_to_retry_queue_full", droppedCount.Load()),
			)
		case <-done:
			return
		}
	}
}

func newAsyncProducer() (sarama.AsyncProducer, error) {
	conf := sarama.NewConfig()
	conf.ClientID = "producer"
	conf.Producer.Flush.Messages = 5000
	conf.Producer.Flush.Bytes = 1 * 1024 * 1024
	conf.Producer.Return.Errors = true
	conf.Producer.Return.Successes = true
	conf.Producer.Flush.Frequency = 200 * time.Millisecond
	conf.Producer.Compression = sarama.CompressionLZ4
	conf.Producer.CompressionLevel = 1
	conf.Producer.RequiredAcks = sarama.WaitForLocal
	conf.ChannelBufferSize = 2048
	conf.Producer.MaxMessageBytes = 10 * 1024 * 1024
	conf.Net.MaxOpenRequests = 10

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	return sarama.NewAsyncProducer([]string{broker}, conf)
}
