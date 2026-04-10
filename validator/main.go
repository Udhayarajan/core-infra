package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"core-infra/validator/handler"

	"github.com/IBM/sarama"
	"golang.org/x/sync/errgroup"
)

var (
	debug bool
)

func init() {
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
	// delete result/sccess.txt if exists
	_ = os.Remove("./results/SUCCESS.txt")
	start := time.Now()
	subscriber := &Subscriber{}
	if err := subscriber.Subscribe(context.Background()); err != nil {
		panic(err)
	}

	slog.Info(
		"Done writing events to files, please check ./csv directory on host machine",
		slog.String("duration", time.Since(start).String()),
		slog.Any("id", "./csv/id_sorted.csv"),
		slog.Any("name", "./csv/name_sorted.csv"),
		slog.Any("continent", "./csv/continent_sorted.csv"),
	)

	printSample()
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
func printSample() {
	outFile, err := os.Create("./results/SUCCESS.txt")
	if err != nil {
		panic(err)
	}
	defer func() { _ = outFile.Close() }()

	mu := sync.Mutex{}
	paths := []string{"./csv/id_sorted.csv", "./csv/name_sorted.csv", "./csv/continent_sorted.csv"}
	eg := errgroup.Group{}
	for _, path := range paths {
		p := path
		eg.Go(func() error {
			return printSampleOn(p, outFile, &mu)
		})
	}
	if err := eg.Wait(); err != nil {
		panic(err)
	}
}

func printSampleOn(path string, out *os.File, mu *sync.Mutex) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	writeLine := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(out, format+"\n", args...)
	}

	writeLine("start sampling path=%s", path)

	// --- HEAD: just read first 3 lines sequentially ---
	writeLine("=== head section === path=%s", path)
	scanner := bufio.NewScanner(file)
	for i := 0; i < 3 && scanner.Scan(); i++ {
		writeLine("[head] path=%s line=%s", path, scanner.Text())
	}

	// --- COUNT total lines ---
	totalLines := 3
	for scanner.Scan() {
		totalLines++
	}
	writeLine("total lines path=%s total_lines=%d", path, totalLines)

	// --- MIDDLE ---
	writeLine("=== mid section === path=%s", path)
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner = bufio.NewScanner(file)
	midStart := (totalLines / 2) - 1
	for i := 0; i < midStart; i++ {
		scanner.Scan()
	}
	for i := 0; i < 3 && scanner.Scan(); i++ {
		writeLine("[mid] path=%s line=%s", path, scanner.Text())
	}

	// --- TAIL ---
	writeLine("=== tail section === path=%s", path)
	const chunkSize = 4096
	var tailBuf []byte
	newlineCount := 0
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	fileSize := offset

	for offset > 0 && newlineCount < 4 {
		step := int64(chunkSize)
		if step > offset {
			step = offset
		}
		offset -= step
		chunk := make([]byte, step)
		if _, err := file.ReadAt(chunk, offset); err != nil {
			return err
		}
		for i := len(chunk) - 1; i >= 0; i-- {
			if chunk[i] == '\n' {
				newlineCount++
				if newlineCount == 4 {
					tailBuf = append(chunk[i+1:], tailBuf...)
					offset += int64(i + 1)
					break
				}
			}
		}
		if newlineCount < 4 {
			tailBuf = append(chunk, tailBuf...)
		}
	}

	if newlineCount < 4 {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		all, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		tailBuf = all
		_ = fileSize
	}

	tailScanner := bufio.NewScanner(bytes.NewReader(tailBuf))
	for tailScanner.Scan() {
		writeLine("[tail] path=%s line=%s", path, tailScanner.Text())
	}

	return nil
}
