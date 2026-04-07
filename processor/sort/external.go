package sort

import (
	"bufio"
	"container/heap"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"core-infra/common"

	"github.com/IBM/sarama"
)

func (s *Sorter) ExternalSort(producer sarama.AsyncProducer) error {
	for sortedPath, by := range sortFuncMapper {
		sortedFullPath := filepath.Join(s.rootPath, sortedPath)
		entries, err := os.ReadDir(sortedFullPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		slog.Info("doing external sort", slog.Any("path", sortedFullPath))

		var files []string
		for _, file := range entries {
			if file.IsDir() {
				continue
			}

			files = append(files, filepath.Join(sortedFullPath, file.Name()))
		}

		sort.Strings(files)
		topic := strings.Split(sortedPath, "_")[1] // sorted_id => id
		slog.Info("merging files", slog.Any("topic", topic), slog.Any("num_files", len(files)))
		if err := mergeFiles(files, topic, producer, by); err != nil {
			return err
		}
	}

	return nil
}

func mergeFiles(files []string, topic string, producer sarama.AsyncProducer, less func(a *common.CSV, b *common.CSV) bool) error {
	readers := make([]*bufio.Scanner, len(files))

	for i, f := range files {
		file, _ := os.Open(f)
		scanner := bufio.NewScanner(file)
		readers[i] = scanner
	}

	h := &MinHeap{less: less}
	heap.Init(h)

	for i, r := range readers {
		if r.Scan() {
			line := append([]byte(nil), r.Bytes()...)
			csv := common.NewFromBytes(line, false)
			heap.Push(h, &Item{value: csv, file: i})
		}
	}

	for h.Len() > 0 {
		item := heap.Pop(h).(*Item)

		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: item.value,
		}

		r := readers[item.file]
		if r.Scan() {
			line := append([]byte(nil), r.Bytes()...)
			csv := common.NewFromBytes(line, false)
			heap.Push(h, &Item{value: csv, file: item.file})
		}
	}

	return nil
}
