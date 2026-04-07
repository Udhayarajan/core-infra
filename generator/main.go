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

var totalMessages int64

const (
	progressInterval = 2 * time.Second
	progressBatch    = int64(1024)
)

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
	})))

	flag.Int64Var(&totalMessages, "total", 50_000_000, "total number of messages to produce")
	flag.Parse()
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
	totalErrors := atomic.Int64{}
	totalProduced := atomic.Int64{}
	slog.Info("Starting producer", slog.Any("total_messages", totalMessages))
	go func() {
		for err := range producer.Errors() {
			fmt.Println("error:", err)
			totalErrors.Add(1)
		}
	}()

	start := time.Now()
	progressDone := make(chan struct{})
	go func() {
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

				slog.Info("progress",
					slog.Int64("produced", produced),
					slog.Int64("total", totalMessages),
					slog.Float64("percent", pct),
					slog.Int64("errors", totalErrors.Load()),
					slog.Any("elapsed", time.Since(start).String()),
					slog.Float64("throughput_msg_per_sec", rate),
				)
			case <-progressDone:
				return
			}
		}
	}()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			gen := data.NewGenerator(rng)
			localProduced := int64(0)

			for j := int64(0); j < dataPerWorker; j++ {
				payload := gen.Generate()
				producer.Input() <- &sarama.ProducerMessage{
					Topic: "source",
					Value: payload,
				}
				localProduced++
				if localProduced >= progressBatch {
					totalProduced.Add(localProduced)
					localProduced = 0
				}
			}

			if localProduced > 0 {
				totalProduced.Add(localProduced)
			}
		}()
	}

	wg.Wait()
	close(progressDone)

	success := totalMessages - totalErrors.Load()
	slog.Info("pipeline complete",
		slog.Any("total", totalMessages),
		slog.Any("errors", totalErrors.Load()),
		slog.Any("success", success),
		slog.Any("duration", time.Since(start).String()),
		slog.Any("throughput_msg_per_sec", float64(totalMessages)/time.Since(start).Seconds()),
	)
}

func newAsyncProducer() (sarama.AsyncProducer, error) {
	conf := sarama.NewConfig()
	conf.ClientID = "producer"
	conf.Producer.Return.Errors = true
	conf.Producer.Flush.Messages = 5000
	conf.Producer.Flush.Bytes = 1 << 20
	conf.Producer.Flush.Frequency = 100 * time.Millisecond
	conf.Producer.Compression = sarama.CompressionSnappy
	conf.Producer.RequiredAcks = sarama.WaitForLocal
	conf.ChannelBufferSize = 1024

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	return sarama.NewAsyncProducer([]string{broker}, conf)
}
