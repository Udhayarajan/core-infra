package handler

import (
	"bufio"
	"log/slog"
	"os"
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
	timeout   time.Duration
	timeoutCh chan struct{}
	timer     *time.Timer
}

func NewConsumerGroupHandler() sarama.ConsumerGroupHandler {
	return &consumerGroupHandler{
		timeout:   5 * time.Minute,
		timeoutCh: make(chan struct{}),
	}
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	defer close(h.timeoutCh)
	defer idSortedCSV.Flush()
	defer nameSortedCSV.Flush()
	defer continentSortedCSV.Flush()

	for {
		select {
		case msg := <-claim.Messages():
			h.ensureTimeout()
			slog.Info("received message", slog.Any("topic", msg.Topic), slog.Any("value", string(msg.Value)))
			if msg == nil {
				continue
			}
			switch msg.Topic {
			case "id":
				write(idSortedCSV, append(msg.Value, '\n'))
			case "name":
				write(nameSortedCSV, append(msg.Value, '\n'))
			case "continent":
				write(continentSortedCSV, append(msg.Value, '\n'))
			default:
				continue
			}

			sess.MarkMessage(msg, "")
		case <-h.timeoutCh:
			return nil
		}
	}
}

func write(w bufio.Writer, s []byte) {
	_, err := w.Write(s)
	if err != nil {
		slog.Error("failed to write message to csv", slog.Any("err", err))
	}
}

func (h consumerGroupHandler) ensureTimeout() {
	if h.timer == nil {
		h.timer = time.NewTimer(h.timeout)
		go func() {
			<-h.timer.C
			close(h.timeoutCh)
		}()
	}
	h.timer.Reset(h.timeout)
}
