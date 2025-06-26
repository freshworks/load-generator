package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Generator struct {
	log        *logrus.Entry
	writer     *kafka.Writer
	reader     *kafka.Reader
	o          GeneratorOptions
	stats      *stats.Stats
	ctx        context.Context
	requestrate int
}

type GeneratorOptions struct {
	Brokers      []string
	Topic        string
	MessageValue string
	MessageKey   string
	GroupID      string
	ReadMessages bool
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	}
}

func NewGenerator(id int, options GeneratorOptions, ctx context.Context, requestrate int, s *stats.Stats) *Generator {
	log := logrus.WithFields(logrus.Fields{"Id": id})
	return &Generator{log: log, o: options, stats: s, ctx: ctx, requestrate: requestrate}
}

func (k *Generator) Init() error {
	if k.o.ReadMessages {
		// Initialize a Kafka reader
		k.reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:  k.o.Brokers,
			Topic:    k.o.Topic,
			GroupID:  k.o.GroupID,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		})
		k.log.Debugf("Kafka reader initialized for topic: %s, group: %s", k.o.Topic, k.o.GroupID)
	} else {
		// Initialize a Kafka writer
		k.writer = &kafka.Writer{
			Addr:     kafka.TCP(k.o.Brokers...),
			Topic:    k.o.Topic,
			Balancer: &kafka.LeastBytes{},
		}
		k.log.Debugf("Kafka writer initialized for topic: %s", k.o.Topic)
	}

	return nil
}

func (k *Generator) InitDone() error {
	return nil
}

func (k *Generator) Tick() error {
	startTime := time.Now()
	var err error

	if k.o.ReadMessages {
		// Read messages
		message, err := k.reader.ReadMessage(k.ctx)
		if err != nil {
			k.log.Errorf("Error reading message: %v", err)
		} else {
			k.log.Debugf("Message read: %s", string(message.Value))
		}
	} else {
		// Write messages
		err = k.writer.WriteMessages(k.ctx,
			kafka.Message{
				Key:   []byte(k.o.MessageKey),
				Value: []byte(k.o.MessageValue),
				Time:  time.Now(),
			},
		)
		if err != nil {
			k.log.Errorf("Failed to write message: %v", err)
		}
	}

	// Record metrics
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.CustomTrace
	traceInfo.Key = fmt.Sprintf("%v", k.o.Brokers)
	if k.o.ReadMessages {
		traceInfo.Subkey = fmt.Sprintf("read:%s", k.o.Topic)
	} else {
		traceInfo.Subkey = fmt.Sprintf("write:%s", k.o.Topic)
	}
	traceInfo.Total = time.Since(startTime)

	if err != nil {
		traceInfo.Error = true
	}

	k.stats.RecordMetric(&traceInfo)
	return err
}

func (k *Generator) Finish() error {
	if k.writer != nil {
		if err := k.writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %v", err)
		}
	}

	if k.reader != nil {
		if err := k.reader.Close(); err != nil {
			return fmt.Errorf("failed to close reader: %v", err)
		}
	}

	return nil
}
