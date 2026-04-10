package batcher

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"core-infra/common"
	srt "core-infra/processor/sort"

	"golang.org/x/sync/errgroup"
)

const avgRecordSize = 50

type Batcher struct {
	messages    []*common.CSV
	batches     chan []*common.CSV
	batchClosed atomic.Bool
	wg          sync.WaitGroup

	flushInterval time.Duration
	limit         int64 // in bytes
	maxEventCount int64
	rootPath      string

	check chan bool
	mu    sync.Mutex

	currentSize  int64
	currentCount int64
}

func NewBatcher(maxEventCount, batchLimit int64, flushInterval time.Duration, rootPath string) *Batcher {
	estimatedCapacity := batchLimit / avgRecordSize

	return &Batcher{
		messages:      make([]*common.CSV, 0, estimatedCapacity),
		flushInterval: flushInterval,
		check:         make(chan bool),
		limit:         batchLimit,
		rootPath:      rootPath,
		batches:       make(chan []*common.CSV, 5), // buffer to hold batches before they are flushed
		maxEventCount: maxEventCount,
	}
}

func (b *Batcher) Start(ctx context.Context, closeCh chan struct{}) {
	if err := os.MkdirAll(b.rootPath, 0o755); err != nil {
		slog.Error("mkdir failed", slog.Any("err", err))
		return
	}
	go b.flushBatches(ctx)
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
				slog.Debug("batcher flush limit exceeded", slog.Any("limit", b.limit), slog.Any("currentSize", b.currentSize))
				b.flushBatch(b.getMessages())
				ticker.Reset(b.flushInterval)
			}
		case <-ticker.C:
			slog.Debug("batcher flush interval")
			msg := b.getMessages()
			b.flushBatch(msg)
			if b.currentCount >= b.maxEventCount {
				slog.Info("expected amount of events received", slog.Any("current_count", b.currentCount))
				b.batchClosed.Store(true)
				close(b.batches)
				b.wg.Wait()
				close(closeCh)
				return
			}
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
	if len(messages) == 0 || b.batchClosed.Load() {
		return
	}
	b.wg.Add(1)
	b.batches <- messages

}

func (b *Batcher) flushBatches(ctx context.Context) {
	batchCount := 0
	for {
		select {
		case batch, ok := <-b.batches:
			if !ok && ctx.Err() != nil {
				return
			}
			batchCount++
			func() {
				defer b.wg.Done()
				g := errgroup.Group{}
				for _, msg := range batch {
					msg.ParseAll()
				}

				for sortPath, fn := range srt.FuncMapper {
					path := filepath.Join(b.rootPath, sortPath, fmt.Sprintf("batch_%d.csv", batchCount))
					g.Go(func(path string, fn func(a, b *common.CSV) bool) func() error {
						return func() error {
							return sortAndSave(path, batch, fn)
						}
					}(path, fn))
				}
				if err := g.Wait(); err != nil {
					slog.Error("flush batch fails", slog.Any("err", err))
				}

				slog.Info("flushed batch", slog.Int("messages", len(batch)))
			}()
		case <-ctx.Done():
			slog.Info("batch flush loop exiting due to context cancellation", slog.Any("batch", batchCount))
			return
		}
	}
}

func (b *Batcher) Limit() (int64, time.Duration) {
	return b.limit, b.flushInterval
}

func (b *Batcher) getMessages() []*common.CSV {
	b.mu.Lock()
	defer b.mu.Unlock()
	old := b.messages
	messageCount := len(b.messages)
	b.messages = make([]*common.CSV, 0, messageCount)
	slog.Debug("getting messages from batcher", slog.Int("message_count", messageCount), slog.Any("current_size", b.currentSize))
	b.currentSize = 0
	b.currentCount += int64(messageCount)
	return old
}

func sortAndSave(name string, csvData []*common.CSV, by func(a *common.CSV, b *common.CSV) bool) error {
	copyData := make([]*common.CSV, len(csvData))
	copy(copyData, csvData)
	sort.Slice(copyData, func(i, j int) bool {
		return by(copyData[i], copyData[j])
	})

	if err := write(name, copyData); err != nil {
		return err
	}

	return nil
}

var writeBufPool = sync.Pool{
	New: func() any {
		return bufio.NewWriterSize(nil, 4*1024*1024)
	},
}

func write(name string, data []*common.CSV) error {
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil && !os.IsExist(err) {
		return err
	}

	file, err := os.Create(name)
	if err != nil {
		return err
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("failed to close file", slog.Any("err", err), slog.String("file", name))
		}
	}()

	writer := writeBufPool.Get().(*bufio.Writer)
	writer.Reset(file)
	defer writeBufPool.Put(writer)

	for _, csvData := range data {
		if _, err := writer.Write(csvData.Bytes()); err != nil {
			return err
		}
	}

	return writer.Flush()
}
