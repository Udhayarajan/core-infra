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
	idFile, err := os.OpenFile("csv/id_sorted.csv", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	idSortedCSV = *bufio.NewWriter(idFile)

	nameFile, err := os.OpenFile("csv/name_sorted.csv", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	nameSortedCSV = *bufio.NewWriter(nameFile)

	continentFile, err := os.OpenFile("csv/continent_sorted.csv", os.O_CREATE|os.O_WRONLY, 0644)
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
	defer flush(&idSortedCSV)
	defer flush(&nameSortedCSV)
	defer flush(&continentSortedCSV)

	var (
		timer *time.Timer
	)
	defer func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for {
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}

		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if msg == nil {
				continue
			}

			if timer == nil {
				timer = time.NewTimer(h.timeout)
				slog.Info("starting validator inactivity timer", slog.Any("timeout", h.timeout))
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(h.timeout)
			}

			switch msg.Topic {
			case "id":
				write(&idSortedCSV, append(msg.Value, '\n'))
			case "name":
				write(&nameSortedCSV, append(msg.Value, '\n'))
			case "continent":
				write(&continentSortedCSV, append(msg.Value, '\n'))
			default:
				continue
			}

			sess.MarkMessage(msg, "")
		case <-timeout:
			slog.Info("validator inactivity timeout reached, shutting down")
			h.timeoutOnce.Do(func() {
				close(h.timeoutCh)
			})
			return nil
		}
	}
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
