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
	slog.Info("Starting producer", slog.Any("total_messages", totalMessages))
	go func() {
		for err := range producer.Errors() {
			fmt.Println("error:", err)
			totalErrors.Add(1)
		}
	}()

	start := time.Now()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			gen := data.NewGenerator(rng)

			for j := int64(0); j < dataPerWorker; j++ {
				payload := gen.Generate()
				producer.Input() <- &sarama.ProducerMessage{
					Topic: "source",
					Value: payload,
				}
			}
		}()
	}

	wg.Wait()

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

	return sarama.NewAsyncProducer([]string{"localhost:9092"}, conf)
}
