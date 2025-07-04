package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	kafkainternal "github.com/freshworks/load-generator/internal/kafka"
	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var kafkaCmd = &cobra.Command{
	Use:   "kafka",
	Short: "Kafka load generator",
	Long: `Kafka load generator

Generates Kafka load by producing messages to or consuming messages from a Kafka topic.
Topics will be automatically created if they don't exist.
`,
	Example: `
# Produce messages to a Kafka topic (creates topic if it doesn't exist)
lg kafka --brokers "localhost:9092" --topic "test-topic" --message "Hello World" --requestrate 10 --duration 30s

# Consume messages from a Kafka topic
lg kafka --brokers "localhost:9092" --topic "test-topic" --group "consumer-group-1" --read --requestrate 10 --duration 30s

# Use with SCRAM authentication
lg kafka --brokers "localhost:9093" --topic "scram-topic" --message "Hello SCRAM" --requestrate 10 --duration 30s \
  --username "admin" --password "admin-secret" --sasl-mechanism "SCRAM-SHA-512"
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check if the topic exists and create it if it doesn't
		if kafkaAutoCreateTopic {
			logrus.Infof("Checking if topic '%s' exists and creating it if needed", kafkaTopic)
			if err := ensureTopicExists(); err != nil {
				logrus.Warnf("Failed to ensure topic exists: %v. Will attempt to continue anyway.", err)
			}
		}

		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := kafkainternal.NewOptions()
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

			return kafkainternal.NewGenerator(id, *o, ctx, requestrate, s)
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
	// Topic creation options

	kafkaAutoCreateTopic bool
	kafkaPartitions      int
	kafkaReplicationFactor int
)

// createKafkaDialer creates a Kafka dialer with SASL authentication if credentials are provided
func createKafkaDialer() (*kafka.Dialer, error) {
	// Configure SASL if credentials are provided
	var dialer *kafka.Dialer
	if kafkaUsername != "" && kafkaPassword != "" && kafkaSASLMechanism != "" {
		var mechanism sasl.Mechanism
		var err error
		
		switch kafkaSASLMechanism {
		case "SCRAM-SHA-256":
			mechanism, err = scram.Mechanism(scram.SHA256, kafkaUsername, kafkaPassword)
		case "SCRAM-SHA-512":
			mechanism, err = scram.Mechanism(scram.SHA512, kafkaUsername, kafkaPassword)
		default:
			logrus.Warnf("Unsupported SASL mechanism: %s, defaulting to SCRAM-SHA-512", kafkaSASLMechanism)
			mechanism, err = scram.Mechanism(scram.SHA512, kafkaUsername, kafkaPassword)
		}
		
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %v", err)
		}
		
		var tlsConfig *tls.Config
		if kafkaUseTLS {
			tlsConfig = &tls.Config{}
		}
		
		dialer = &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: mechanism,
			TLS:           tlsConfig,
		}
		logrus.Infof("Using SASL authentication with mechanism: %s", kafkaSASLMechanism)
	} else {
		dialer = &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		}
	}
	
	return dialer, nil
}

// ensureTopicExists checks if a topic exists and creates it if it doesn't
func ensureTopicExists() error {
	logrus.Infof("Ensuring Kafka topic '%s' exists", kafkaTopic)
	
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	
	// Maximum number of retries
	maxRetries := 3
	retryDelay := 2 * time.Second
	
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			logrus.Infof("Retry attempt %d of %d for topic '%s'", attempt, maxRetries, kafkaTopic)
			time.Sleep(retryDelay) // Exponential backoff
			retryDelay *= 2
		}
		
		// Create a Kafka dialer with authentication if needed
		dialer, err := createKafkaDialer()
		if err != nil {
			logrus.Warnf("Failed to create Kafka dialer: %v", err)
			continue
		}
		
		// Connect to the Kafka cluster
		conn, err := dialer.DialContext(ctx, "tcp", kafkaBrokers[0])
		if err != nil {
			logrus.Warnf("Failed to connect to Kafka: %v. Retrying...", err)
			continue
		}
		defer conn.Close()
		
		// Check if the topic exists
		partitions, err := conn.ReadPartitions(kafkaTopic)
		if err == nil && len(partitions) > 0 {
			logrus.Infof("Topic '%s' already exists with %d partitions", kafkaTopic, len(partitions))
			return nil
		}
		
		// Try to create the topic with different replication factors if needed
		for _, replicationFactor := range []int{kafkaReplicationFactor, 1} {
			logrus.Infof("Attempting to create topic '%s' with %d partitions and replication factor %d", 
				kafkaTopic, kafkaPartitions, replicationFactor)
			
			// Create the topic
			topicConfigs := []kafka.TopicConfig{
				{
					Topic:             kafkaTopic,
					NumPartitions:     kafkaPartitions,
					ReplicationFactor: replicationFactor,
				},
			}
			
			err = conn.CreateTopics(topicConfigs...)
			
			// If successful or if error is not related to replication factor, break the inner loop
			if err == nil || !strings.Contains(err.Error(), "InvalidReplicationFactorException") {
				break
			}
			
			logrus.Warnf("Failed to create topic with replication factor %d: %v", replicationFactor, err)
		}
		
		// If we have a controller error, wait and check if the topic was created anyway
		if err != nil && strings.Contains(err.Error(), "Not Controller") {
			logrus.Warnf("Controller error when creating topic: %v", err)
			logrus.Infof("Topic might be created by another controller, waiting and checking")
			
			// Wait longer for controller issues
			time.Sleep(5 * time.Second)
			
			// Try to connect to a different broker if available
			if len(kafkaBrokers) > 1 {
				logrus.Infof("Trying another broker from the list")
				// Close the current connection
				conn.Close()
				
				// Connect to a different broker
				for i, broker := range kafkaBrokers {
					if broker != kafkaBrokers[0] {
						// Swap the first broker with this one for the next attempt
						kafkaBrokers[0], kafkaBrokers[i] = kafkaBrokers[i], kafkaBrokers[0]
						break
					}
				}
				
				// Connect to the new first broker
				conn, err = dialer.DialContext(ctx, "tcp", kafkaBrokers[0])
				if err != nil {
					logrus.Warnf("Failed to connect to alternate broker: %v", err)
					continue
				}
			}
			
			// Check if the topic exists now
			partitions, checkErr := conn.ReadPartitions(kafkaTopic)
			if checkErr == nil && len(partitions) > 0 {
				logrus.Infof("Topic '%s' now exists with %d partitions", kafkaTopic, len(partitions))
				return nil
			}
			
			// If we still can't find the topic, continue to the next attempt
			continue
		}
		
		// If we created the topic successfully
		if err == nil {
			logrus.Infof("Successfully created topic '%s'", kafkaTopic)
			
			// Add a longer delay to ensure the topic is fully ready
			logrus.Infof("Waiting for topic to be fully ready...")
			time.Sleep(5 * time.Second)
			
			// Verify the topic is now accessible
			partitions, verifyErr := conn.ReadPartitions(kafkaTopic)
			if verifyErr == nil && len(partitions) > 0 {
				logrus.Infof("Topic '%s' is now ready with %d partitions", kafkaTopic, len(partitions))
				return nil
			} else {
				logrus.Warnf("Topic creation succeeded but topic verification failed: %v", verifyErr)
				// Continue to the next attempt
			}
		} else {
			logrus.Warnf("Failed to create topic: %v", err)
		}
	}
	
	// If we've exhausted all retries, return a generic error
	return fmt.Errorf("failed to create topic after %d attempts", maxRetries)
}



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
	
	// Topic creation options
	kafkaCmd.Flags().BoolVar(&kafkaAutoCreateTopic, "auto-create-topic", true, "Automatically create the topic if it doesn't exist before producing/consuming")
	kafkaCmd.Flags().IntVar(&kafkaPartitions, "partitions", 1, "Number of partitions when creating a topic")
	kafkaCmd.Flags().IntVar(&kafkaReplicationFactor, "replication-factor", 1, "Replication factor when creating a topic")
}
