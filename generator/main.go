package main

import (
	"flag"
	"fmt"
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
	totalMessages int64
	totalProduced atomic.Int64
	totalErrors   atomic.Int64
	debug         bool
)

const (
	progressInterval = 5 * time.Second
)

func init() {
	flag.Int64Var(&totalMessages, "max", 50_000_000, "maximum number of messages or event to be generated")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
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

	numWorkers := runtime.NumCPU()
	dataPerWorker := totalMessages / int64(numWorkers)

	var wg sync.WaitGroup
	slog.Info("Starting producer", slog.Any("total_messages", totalMessages))
	go func() {
		for err := range producer.Errors() {
			fmt.Println("error:", err)
			totalErrors.Add(1)
		}
	}()

	start := time.Now()
	progressDone := make(chan struct{})
	go progressLogger(progressDone, start)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go generateDataWorker(&wg, dataPerWorker, producer)
	}

	wg.Wait()
	close(progressDone)

	produced := totalProduced.Load()
	success := produced - totalErrors.Load()
	slog.Info("pipeline complete",
		slog.Any("total", produced),
		slog.Any("errors", totalErrors.Load()),
		slog.Any("success", success),
		slog.String("duration", time.Since(start).String()),
		slog.Any("throughput_msg_per_sec", float64(produced)/time.Since(start).Seconds()),
	)
}

func generateDataWorker(wg *sync.WaitGroup, dataPerWorker int64, producer sarama.AsyncProducer) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	gen := data.NewGenerator(rng)

	for j := int64(0); j <= dataPerWorker; j++ {
		payload := gen.Generate()
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "source",
			Value: payload,
		}
		totalProduced.Add(1)
	}
}

func progressLogger(done chan struct{}, start time.Time) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			produced := totalProduced.Load()
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
				slog.Int64("total", totalMessages),
				slog.Float64("percent", pct),
				slog.Int64("errors", totalErrors.Load()),
				slog.Any("elapsed", time.Since(start).String()),
				slog.Float64("throughput_msg_per_sec", rate),
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
	conf.Producer.Flush.Messages = 5000
	conf.Producer.Flush.Bytes = 1 << 20
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
