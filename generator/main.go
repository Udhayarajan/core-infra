package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"core-infra/common"

	"github.com/IBM/sarama"
)

type Continent string

var (
	Asia         Continent = "Asia"
	Africa       Continent = "Africa"
	Australia    Continent = "Australia"
	Europe       Continent = "Europe"
	NorthAmerica Continent = "North America"
	SouthAmerica Continent = "South America"
)

var availableContinents = []Continent{
	Asia,
	Africa,
	Australia,
	Europe,
	NorthAmerica,
	SouthAmerica,
}

const (
	minNameLength              = 10
	maxNameLength              = 15
	minAddressLength           = 15
	maxAddressLength           = 20
	alphabetCharset            = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	alphabetNumberSpaceCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
)

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func generateCSV() *common.CSV {
	nameLength := randomLength(minNameLength, maxNameLength)
	addressLength := randomLength(minAddressLength, maxAddressLength)

	b := make([]byte, 0, 64)

	b = strconv.AppendInt(b, int64(rng.Int31()), 10)
	b = append(b, ',')

	name := randomString(nameLength, alphabetCharset)
	b = append(b, name...)
	b = append(b, ',')

	address := randomString(addressLength, alphabetNumberSpaceCharset)
	b = append(b, address...)
	b = append(b, ',')

	cont := availableContinents[rng.Intn(len(availableContinents))]
	b = append(b, cont...)

	return common.NewFromBytes(b, false)
}

func randomLength(min, max int) int {
	return rng.Intn(max-min+1) + min
}

func randomString(length int, charset string) string {
	var b strings.Builder
	b.Grow(length)

	for i := 0; i < length; i++ {
		b.WriteByte(charset[rng.Intn(len(charset))])
	}

	return b.String()
}

func main() {
	total := 1_000_000
	producer, err := NewAsyncProducer()
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(total)
	totalErrors := atomic.Int32{}
	slog.Info("Starting producer", slog.Any("total_messages", total))
	go func() {
		for err := range producer.Errors() {
			fmt.Println("error:", err)
			totalErrors.Add(1)
			wg.Done()
		}
	}()

	go func() {
		for range producer.Successes() {
			wg.Done()
		}
	}()
	start := time.Now()
	for i := 0; i < total; i++ {
		payload := generateCSV()
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "source",
			Value: payload,
		}
	}

	wg.Wait()

	success := total - int(totalErrors.Load())
	fmt.Println("PRODUCER STAT")
	fmt.Println("Total:", total)
	fmt.Println("Producer Errors:", totalErrors.Load())
	fmt.Println("Successes:", success)
	fmt.Println("Duration:", time.Since(start))
	fmt.Println("Throughput:", float64(success)/time.Since(start).Seconds(), "msg/sec")
}

func NewAsyncProducer() (sarama.AsyncProducer, error) {
	conf := sarama.NewConfig()
	conf.ClientID = "producer"
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	conf.Producer.Flush.Messages = 1000
	conf.Producer.Flush.Frequency = 500 * time.Millisecond
	conf.Producer.Compression = sarama.CompressionSnappy
	conf.Producer.RequiredAcks = sarama.WaitForLocal

	client, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, conf)
	if err != nil {
		return nil, err
	}

	return client, nil
}
