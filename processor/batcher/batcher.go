package batcher

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"core-infra/common"
)

type Batcher struct {
	messages      []*common.CSV
	flushInterval time.Duration
	limit         int64
	currentSize   int64
	check         chan bool
	batchCount    int
	rootPath      string
	mu            sync.Mutex
}

func NewBatcher(batchLimit int64, flushInterval time.Duration, runID string) *Batcher {
	return &Batcher{
		messages:      make([]*common.CSV, 0, batchLimit),
		flushInterval: flushInterval,
		check:         make(chan bool),
		limit:         batchLimit,
		rootPath:      fmt.Sprintf("csv/%s", runID),
	}
}

func (b *Batcher) Start(ctx context.Context) {
	if err := os.MkdirAll(b.rootPath, 0o755); err != nil {
		slog.Error("mkdir failed", slog.Any("err", err))
		return
	}
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			b.flushBatch(b.getMessages())
			return
		case <-b.check:
			b.mu.Lock()
			shouldFlush := b.currentSize >= b.limit
			b.mu.Unlock()
			if shouldFlush {
				slog.Info("batcher flush limit exceeded", slog.Any("limit", b.limit), slog.Any("currentSize", b.currentSize))
				b.flushBatch(b.getMessages())
				ticker.Reset(b.flushInterval)
			}
		case <-ticker.C:
			slog.Info("batcher flush interval")
			b.flushBatch(b.getMessages())
			ticker.Reset(b.flushInterval)
		}
	}
}

func (b *Batcher) AddMessage(msg *common.CSV) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = append(b.messages, msg)
	b.currentSize += int64(len(msg.Bytes()))
	select {
	case b.check <- true:
	default:
	}
}

func (b *Batcher) flushBatch(messages []*common.CSV) {
	if len(messages) == 0 {
		return
	}

	b.batchCount++
	path := fmt.Sprintf("%s/batch_%d.csv", b.rootPath, b.batchCount)

	f, err := os.Create(path)
	if err != nil {
		slog.Error("open batch file failed", slog.Any("err", err), slog.String("path", path))
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	written := 0
	for _, msg := range messages {
		_, err := w.Write(msg.Bytes())
		if err != nil {
			slog.Error("write batch failed", slog.Any("err", err))
		}
		written++
	}

	if err := w.Flush(); err != nil {
		slog.Error("flush batch file failed", slog.Any("err", err))
	}
	slog.Info("flushed batch", slog.String("path", path), slog.Int("messages", written))
}

func (b *Batcher) Limit() (int64, time.Duration) {
	return b.limit, b.flushInterval
}

func (b *Batcher) getMessages() []*common.CSV {
	b.mu.Lock()
	defer b.mu.Unlock()
	old := b.messages
	b.messages = make([]*common.CSV, 0, b.limit)
	b.currentSize = 0
	return old
}
