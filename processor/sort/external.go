package sort

import (
	"bufio"
	"container/heap"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"core-infra/common"

	"github.com/IBM/sarama"
	"golang.org/x/sync/errgroup"
)

const mergeProgressLogInterval = 45 * time.Second

func ExternalSort(rootPath string, producer sarama.AsyncProducer) error {
	eg := errgroup.Group{}
	for sortedPath, by := range FuncMapper {
		sortedFullPath := filepath.Join(rootPath, sortedPath)
		entries, err := os.ReadDir(sortedFullPath)
		if err != nil {
			if os.IsNotExist(err) {
				slog.Debug("sorter skipped dir does not exist", slog.String("path", sortedFullPath))
				continue
			}
			slog.Error("failed to read sorted directory", slog.Any("path", sortedFullPath), slog.Any("err", err))
			return err
		}

		topic := topicFromSortedPath(sortedPath)
		slog.Info("doing external sort", slog.String("path", sortedFullPath), slog.String("topic", topic))

		var files []string
		for _, file := range entries {
			if file.IsDir() {
				slog.Debug("skipping dir", slog.String("path", filepath.Join(sortedFullPath, file.Name())))
				continue
			}

			files = append(files, filepath.Join(sortedFullPath, file.Name()))
		}

		eg.Go(func(files []string, topic string, less func(a *common.CSV, b *common.CSV) bool) func() error {
			return func() error {
				sort.Strings(files)
				slog.Info("merging files", slog.String("topic", topic), slog.Int("num_files", len(files)))
				return mergeFiles(files, topic, producer, less)
			}
		}(files, topic, by))
	}

	return eg.Wait()
}

type mergeSource struct {
	name    string
	file    *os.File
	scanner *bufio.Scanner
}

func mergeFiles(files []string, topic string, producer sarama.AsyncProducer, less func(a *common.CSV, b *common.CSV) bool) error {
	if len(files) == 0 {
		slog.Debug("no merge files found", slog.String("topic", topic))
		return nil
	}

	readers := make([]mergeSource, len(files))
	for i, f := range files {
		file, err := os.Open(f)
		if err != nil {
			return err
		}

		readers[i] = mergeSource{name: f, file: file, scanner: bufio.NewScanner(file)}
	}

	slog.Info("starting external merge", slog.String("topic", topic), slog.Int("num_files", len(files)))

	h := &MinHeap{less: less}
	heap.Init(h)
	emitted := 0
	lastProgressLog := time.Now()

	for i, r := range readers {
		if r.scanner.Scan() {
			line := append([]byte(nil), r.scanner.Bytes()...)
			csv := common.NewFromBytes(line, true)
			heap.Push(h, &Item{value: csv, file: i})
			continue
		}

		if err := r.scanner.Err(); err != nil {
			return err
		}
	}

	slog.Info("seeded external merge heap", slog.String("topic", topic), slog.Int("initial_items", h.Len()))

	logProgress := func(force bool) {
		if !force && emitted > 0 && time.Since(lastProgressLog) < mergeProgressLogInterval {
			return
		}

		slog.Info("external merge progress",
			slog.String("topic", topic),
			slog.Int("emitted", emitted),
			slog.Int("queued", h.Len()),
		)
		lastProgressLog = time.Now()
	}

	for h.Len() > 0 {
		item := heap.Pop(h).(*Item)

		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: item.value,
		}
		emitted++
		logProgress(false)

		r := readers[item.file]
		if r.scanner.Scan() {
			line := append([]byte(nil), r.scanner.Bytes()...)
			csv := common.NewFromBytes(line, true)
			heap.Push(h, &Item{value: csv, file: item.file})
			continue
		}

		if err := r.scanner.Err(); err != nil {
			return err
		}
		slog.Debug("merge source exhausted", slog.String("topic", topic), slog.String("file", r.name), slog.Int("emitted", emitted))
	}

	logProgress(true)
	slog.Info("finished external merge", slog.String("topic", topic), slog.Int("emitted", emitted))

	return nil
}

func topicFromSortedPath(sortedPath string) string {
	if _, topic, ok := strings.Cut(sortedPath, "_"); ok && topic != "" {
		return topic
	}

	return sortedPath
}
