package kafka

import (
	"io/ioutil"
	"log"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/net/context"
)

type NozzleProducer interface {
	// Produce produces firehose events
	Produce(context.Context)

	// Errors returns error channel
	Errors() <-chan *kafka.Error

	// Success returns sarama.ProducerMessage
	Successes() <-chan *kafka.Message

	// Close shuts down the producer and flushes any messages it may have buffered.
	Close()

	ReadDeliveryChan()
}

var defaultLogger = log.New(ioutil.Discard, "", log.LstdFlags)

// LogProducer implements NozzleProducer interfaces.
// This producer is mainly used for debugging reason.
type LogProducer struct {
	Logger *log.Logger

	once sync.Once
}

func NewLogProducer(logger *log.Logger) NozzleProducer {
	return &LogProducer{
		Logger: logger,
	}
}

// init sets default logger
func (p *LogProducer) init() {
	if p.Logger == nil {
		p.Logger = defaultLogger
	}
}

func (p *LogProducer) Produce(ctx context.Context) {
	p.once.Do(p.init)
	for {
		select {
		/*case event := <-eventCh:
		buf, _ := json.Marshal(event)
		p.Logger.Printf("[INFO] %s", string(buf))*/
		case <-ctx.Done():
			p.Logger.Printf("[INFO] Stop producer")
			return
		}
	}
}

func (p *LogProducer) Errors() <-chan *kafka.Error {
	errCh := make(chan *kafka.Error, 1)
	return errCh
}

func (p *LogProducer) Successes() <-chan *kafka.Message {
	msgCh := make(chan *kafka.Message)
	return msgCh
}

func (p *LogProducer) Close() {
	// Nothing to close for thi producer
}

func (p *LogProducer) ReadDeliveryChan() {
	// Nothing to do here
}
