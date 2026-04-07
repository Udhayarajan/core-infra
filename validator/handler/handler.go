package handler

import (
	"bufio"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

var (
	idSortedCSV, nameSortedCSV, continentSortedCSV bufio.Writer
)

func init() {
	err := os.MkdirAll("csv", os.ModePerm)
	if err != nil {
		panic(err)
	}
	idFile, err := os.OpenFile("csv/id_sorted.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	idSortedCSV = *bufio.NewWriter(idFile)

	nameFile, err := os.OpenFile("csv/name_sorted.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	nameSortedCSV = *bufio.NewWriter(nameFile)

	continentFile, err := os.OpenFile("csv/continent_sorted.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	continentSortedCSV = *bufio.NewWriter(continentFile)
}

type consumerGroupHandler struct {
	timeout     time.Duration
	timeoutCh   chan struct{}
	timeoutOnce sync.Once
}

func NewConsumerGroupHandler(ch chan struct{}) sarama.ConsumerGroupHandler {
	return &consumerGroupHandler{
		timeout:   20 * time.Second,
		timeoutCh: ch,
	}
}

func (*consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	defer flushAll()

	var (
		timer *time.Timer
	)

	defer stopAndDrainTimer(timer)

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if msg == nil {
				continue
			}

			timer = h.startOrResetTimer(timer)

			if !writeToTopicCSV(msg) {
				continue
			}

			sess.MarkMessage(msg, "")
		case <-timerChannel(timer):
			h.handleInactivityTimeout()
			return nil
		}
	}
}

func flushAll() {
	flush(&idSortedCSV)
	flush(&nameSortedCSV)
	flush(&continentSortedCSV)
}

func timerChannel(timer *time.Timer) <-chan time.Time {
	if timer == nil {
		return nil
	}

	return timer.C
}

func (h *consumerGroupHandler) startOrResetTimer(timer *time.Timer) *time.Timer {
	if timer == nil {
		slog.Info("starting validator inactivity timer", slog.Any("timeout", h.timeout))
		return time.NewTimer(h.timeout)
	}

	stopAndDrainTimer(timer)
	timer.Reset(h.timeout)
	return timer
}

func stopAndDrainTimer(timer *time.Timer) {
	if timer == nil {
		return
	}

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func writeToTopicCSV(msg *sarama.ConsumerMessage) bool {
	line := append(msg.Value, '\n')

	switch msg.Topic {
	case "id":
		write(&idSortedCSV, line)
	case "name":
		write(&nameSortedCSV, line)
	case "continent":
		write(&continentSortedCSV, line)
	default:
		return false
	}

	return true
}

func (h *consumerGroupHandler) handleInactivityTimeout() {
	slog.Info("validator inactivity timeout reached, shutting down")
	h.timeoutOnce.Do(func() {
		close(h.timeoutCh)
	})
}

func flush(w *bufio.Writer) {
	if err := w.Flush(); err != nil {
		slog.Error("failed to flush csv writer", slog.Any("err", err))
	}
}

func write(w *bufio.Writer, s []byte) {
	_, err := w.Write(s)
	if err != nil {
		slog.Error("failed to write message to csv", slog.Any("err", err))
	}
}
