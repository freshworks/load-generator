package kafka

import (
	"context"
	"testing"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaProducer(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	options := NewOptions()
	options.Brokers = []string{"localhost:9092"}
	options.Topic = "test-topic"
	options.MessageValue = "test-message"
	options.MessageKey = "test-key"
	options.ReadMessages = false

	sts := stats.New("id", 1, 1, 0, false)
	sts.Start()
	defer sts.Stop()

	logger, _ := test.NewNullLogger()
	require.NotNil(logger)

	t.Run("Producer", func(t *testing.T) {
		g := NewGenerator(0, *options, context.Background(), 1, sts)
		require.NotNil(g)
		sts.Reset()

		// Skip actual initialization since we don't want to connect to Kafka in tests
		// Just verify the struct is properly set up
		assert.Equal(options.Topic, g.o.Topic)
		assert.Equal(options.Brokers, g.o.Brokers)
		assert.Equal(options.MessageKey, g.o.MessageKey)
		assert.Equal(options.MessageValue, g.o.MessageValue)
		assert.Equal(false, g.o.ReadMessages)

		// We're not testing the actual connection to Kafka
		// Just verifying the struct is set up correctly

		// No need to call Finish() as we didn't actually connect
	})
}

func TestKafkaConsumer(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	options := NewOptions()
	options.Brokers = []string{"localhost:9092"}
	options.Topic = "test-topic"
	options.GroupID = "test-group"
	options.ReadMessages = true

	sts := stats.New("id", 1, 1, 0, false)
	sts.Start()
	defer sts.Stop()

	logger, _ := test.NewNullLogger()
	require.NotNil(logger)

	t.Run("Consumer", func(t *testing.T) {
		g := NewGenerator(0, *options, context.Background(), 1, sts)
		require.NotNil(g)
		sts.Reset()

		// Skip actual initialization since we don't want to connect to Kafka in tests
		// Just verify the struct is properly set up
		assert.Equal(options.Topic, g.o.Topic)
		assert.Equal(options.Brokers, g.o.Brokers)
		assert.Equal(options.GroupID, g.o.GroupID)
		assert.Equal(true, g.o.ReadMessages)

		// We're not testing the actual connection to Kafka
		// Just verifying the struct is set up correctly

		// No need to call Finish() as we didn't actually connect
	})
}

func getStatResultsFor(s *stats.Stats) []stats.Result {
	r := s.Export()
	return r.Results
}
