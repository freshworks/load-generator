package cmd

import (
	"context"

	"github.com/freshworks/load-generator/internal/kafka"
	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/spf13/cobra"
)

var kafkaCmd = &cobra.Command{
	Use:   "kafka",
	Short: "Kafka load generator",
	Long: `Kafka load generator

Generates Kafka load by producing messages to or consuming messages from a Kafka topic.
`,
	Example: `
# Produce messages to a Kafka topic
lg kafka --brokers "localhost:9092" --topic "test-topic" --message "Hello World" --requestrate 10 --duration 30s

# Consume messages from a Kafka topic
lg kafka --brokers "localhost:9092" --topic "test-topic" --group "consumer-group-1" --read --requestrate 10 --duration 30s
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := kafka.NewOptions()
			o.Brokers = kafkaBrokers
			o.Topic = kafkaTopic
			o.MessageValue = kafkaMessage
			o.MessageKey = kafkaKey
			o.GroupID = kafkaGroup
			o.ReadMessages = kafkaRead
			
			// Set SCRAM authentication options if username is provided
			if kafkaUsername != "" {
				o.Username = kafkaUsername
				o.Password = kafkaPassword
				o.SASLMechanism = kafkaSASLMechanism
				o.UseTLS = kafkaUseTLS
			}

			return kafka.NewGenerator(id, *o, ctx, requestrate, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var (
	kafkaBrokers []string
	kafkaTopic   string
	kafkaMessage string
	kafkaKey     string
	kafkaGroup   string
	kafkaRead    bool
	// SCRAM authentication options
	kafkaUsername     string
	kafkaPassword     string
	kafkaSASLMechanism string
	kafkaUseTLS       bool
)

func init() {
	rootCmd.AddCommand(kafkaCmd)
	kafkaCmd.Flags().StringSliceVar(&kafkaBrokers, "brokers", []string{"localhost:9092"}, "Kafka broker addresses (comma-separated)")
	kafkaCmd.Flags().StringVar(&kafkaTopic, "topic", "test-topic", "Kafka topic name")
	kafkaCmd.Flags().StringVar(&kafkaMessage, "message", "Test message", "Message content to produce")
	kafkaCmd.Flags().StringVar(&kafkaKey, "key", "", "Message key (optional)")
	kafkaCmd.Flags().StringVar(&kafkaGroup, "group", "lg-consumer", "Consumer group ID (only used with --read)")
	kafkaCmd.Flags().BoolVar(&kafkaRead, "read", false, "Read messages instead of producing them")
	
	// SCRAM authentication options
	kafkaCmd.Flags().StringVar(&kafkaUsername, "username", "", "SASL username for authentication")
	kafkaCmd.Flags().StringVar(&kafkaPassword, "password", "", "SASL password for authentication")
	kafkaCmd.Flags().StringVar(&kafkaSASLMechanism, "sasl-mechanism", "SCRAM-SHA-512", "SASL mechanism (SCRAM-SHA-256 or SCRAM-SHA-512)")
	kafkaCmd.Flags().BoolVar(&kafkaUseTLS, "tls", false, "Enable TLS for Kafka connections")
}
