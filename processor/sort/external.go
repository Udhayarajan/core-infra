package sort

import (
	"bufio"
	"bytes"
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
				return mergeFiles(files, topic, producer, less)
			}
		}(files, topic, by))
	}

	return eg.Wait()
}

type mergeSource struct {
	name   string
	file   *os.File
	reader *bufio.Scanner
}

func mergeFiles(files []string, topic string, producer sarama.AsyncProducer, less func(a *common.CSV, b *common.CSV) bool) error {
	if len(files) == 0 {
		slog.Info("no merge files found", slog.String("topic", topic))
		return nil
	}

	readers, err := openMergeSources(files)
	if err != nil {
		return err
	}
	// ensure files are closed when done
	defer closeMergeSources(readers)

	slog.Info("starting external merge", slog.String("topic", topic), slog.Int("num_files", len(files)))

	h := &MinHeap{less: less}
	heap.Init(h)
	emitted := 0
	lastProgressLog := time.Now()

	// seed heap with the first record from each reader
	for i := range readers {
		if err := pushNext(h, readers, i); err != nil {
			return err
		}
	}

	slog.Info("seeded external merge heap", slog.String("topic", topic), slog.Int("initial_items", h.Len()))

	logProgress := func(force bool) {
		if !force && emitted > 0 && time.Since(lastProgressLog) < mergeProgressLogInterval {
			return
		}

		slog.Debug("external merge progress", slog.String("topic", topic), slog.Int("emitted", emitted), slog.Int("queued", h.Len()))
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

		if err := pushNext(h, readers, item.file); err != nil {
			return err
		}
	}

	logProgress(true)
	slog.Info("finished external merge", slog.String("topic", topic), slog.Int("emitted", emitted))

	return nil
}

func openMergeSources(files []string) ([]mergeSource, error) {
	readers := make([]mergeSource, len(files))
	for i, f := range files {
		file, err := os.Open(f)
		if err != nil {
			for j := 0; j < i; j++ {
				if readers[j].file != nil {
					_ = readers[j].file.Close()
				}
			}
			return nil, err
		}
		reader := bufio.NewScanner(file)

		readers[i] = mergeSource{name: f, file: file, reader: reader}
	}
	return readers, nil
}

// closeMergeSources closes all files in readers.
func closeMergeSources(readers []mergeSource) {
	for _, r := range readers {
		if r.file != nil {
			_ = r.file.Close()
		}
	}
}

func pushNext(h *MinHeap, readers []mergeSource, idx int) error {
	r := readers[idx]
	ok := r.reader.Scan()
	if !ok {
		return nil
	}
	lineBytes := append([]byte(nil), r.reader.Bytes()...) // copy since scanner buffer will be reused
	push(h, lineBytes, idx)
	return nil
}

func push(h *MinHeap, readBytes []byte, idx int) {
	line := bytes.TrimRight(readBytes, "\r\n")
	csv := common.NewFromBytes(line, true)
	heap.Push(h, &Item{value: csv, file: idx})
}

func topicFromSortedPath(sortedPath string) string {
	if _, topic, ok := strings.Cut(sortedPath, "_"); ok && topic != "" {
		return topic
	}

	return sortedPath
}
