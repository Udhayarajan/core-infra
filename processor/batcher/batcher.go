package batcher

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"core-infra/common"
)

type Batcher struct {
	messages      chan *common.CSV
	flushInterval time.Duration
	limit         int
	check         chan bool
	batchCount    int
	runID         string
}

func NewBatcher(batchLimit int, flushInterval time.Duration, runID string) *Batcher {
	return &Batcher{
		messages:      make(chan *common.CSV, batchLimit),
		flushInterval: flushInterval,
		check:         make(chan bool),
		limit:         batchLimit,
		runID:         runID,
	}
}

func (b *Batcher) Start(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.check:
			if len(b.messages) == b.limit {
				slog.Info("batcher flush limit exceeded")
				b.flush()
				ticker.Reset(b.flushInterval)
			}
		case <-ticker.C:
			slog.Info("batcher flush interval")
			b.flush()
			ticker.Reset(b.flushInterval)
		}
	}
}

func (b *Batcher) AddMessage(msg *common.CSV) {
	b.messages <- msg
	select {
	case b.check <- true:
	default:
	}
}

func (b *Batcher) flush() {
	if len(b.messages) == 0 {
		return
	}

	if err := os.MkdirAll(fmt.Sprintf("csv/%s", b.runID), 0o755); err != nil {
		slog.Error("mkdir failed", slog.Any("err", err))
		return
	}

	path := fmt.Sprintf("csv/%s/batch_%d.csv", b.runID, b.batchCount)
	csvBatchFile, err := os.Create(path)
	if err != nil {
		slog.Error("open batch file failed", slog.Any("err", err), slog.String("path", path))
		return
	}

	slog.Info("created batch file", slog.String("path", path))

	defer func() {
		if err := csvBatchFile.Close(); err != nil {
			slog.Error("close batch file failed", slog.Any("err", err))
		}
	}()

	defer func() {
		b.batchCount++
	}()

	for {
		select {
		case msg, ok := <-b.messages:
			if !ok {
				return
			}
			if _, err := csvBatchFile.Write(append(msg.Bytes(), '\n')); err != nil {
				slog.Error(err.Error())
			}
		default:
			return
		}
	}
}

func (b *Batcher) Limit() (int, time.Duration) {
	return b.limit, b.flushInterval
}
