package handler

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
)

const writeBufferSize = 20 * 1024 * 1024 // 20MB

// csvWriters holds buffered writers and their underlying files for clean lifecycle management.
type csvWriters struct {
	writers map[string]*bufio.Writer
	files   []*os.File
}

func newCSVWriters(dir string, topics []string) (*csvWriters, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create csv dir: %w", err)
	}

	cw := &csvWriters{
		writers: make(map[string]*bufio.Writer, len(topics)),
	}

	for _, topic := range topics {
		path := fmt.Sprintf("%s/%s_sorted.csv", dir, topic)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			cw.Close() // clean up already-opened files
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		cw.files = append(cw.files, f)
		cw.writers[topic] = bufio.NewWriterSize(f, writeBufferSize)
	}

	return cw, nil
}

func (cw *csvWriters) Write(topic string, line []byte) bool {
	w, ok := cw.writers[topic]
	if !ok {
		return false
	}
	if _, err := w.Write(line); err != nil {
		slog.Error("failed to write message to csv", slog.String("topic", topic), slog.Any("err", err))
	}
	return true
}

func (cw *csvWriters) Flush() {
	slog.Info("flushing writers...")
	for topic, w := range cw.writers {
		if err := w.Flush(); err != nil {
			slog.Error("failed to flush csv writer", slog.String("topic", topic), slog.Any("err", err))
		}
	}
}

// Close flushes all writers and closes the underlying files.
func (cw *csvWriters) Close() {
	slog.Info("closing writers...")
	cw.Flush()
	for _, f := range cw.files {
		if err := f.Close(); err != nil {
			slog.Error("failed to close csv file", slog.String("file", f.Name()), slog.Any("err", err))
		}
	}
}
