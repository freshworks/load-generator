package cmd

import (
	"testing"
	"os/exec"
	"time"
	"strings"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKafkaTopicExists tests the scenario where a topic already exists
func TestKafkaTopicExists(t *testing.T) {
	// Skip this test if not running in CI or if KAFKA_TEST_ENABLED is not set
	if os.Getenv("KAFKA_TEST_ENABLED") != "true" {
		t.Skip("Skipping Kafka test; set KAFKA_TEST_ENABLED=true to enable")
	}

	// Build the binary if it doesn't exist
	binaryPath := filepath.Join("..", "load-generator")
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		cmd := exec.Command("go", "build", "-o", binaryPath, "../main.go")
		err := cmd.Run()
		require.NoError(t, err, "Failed to build binary")
	}

	// Start Kafka using docker compose
	startKafka := exec.Command("docker", "compose", "up", "-d", "zookeeper", "kafka")
	err := startKafka.Run()
	require.NoError(t, err, "Failed to start Kafka")

	// Ensure Kafka is cleaned up after the test
	defer func() {
		stopKafka := exec.Command("docker", "compose", "down")
		_ = stopKafka.Run()
	}()

	// Wait for Kafka to start
	time.Sleep(15 * time.Second)

	// Define topic name for this test
	topicName := "test-existing-topic"

	// First run: Create the topic
	createCmd := exec.Command(
		binaryPath, "kafka",
		"--brokers", "localhost:9092",
		"--topic", topicName,
		"--message", "Create topic",
		"--requestrate", "1",
		"--duration", "1s",
	)
	createOutput, err := createCmd.CombinedOutput()
	require.NoError(t, err, "Failed to create topic: %s", string(createOutput))

	// Wait for topic creation to complete
	time.Sleep(2 * time.Second)

	// Second run: Use the same topic to test existing topic handling
	existingCmd := exec.Command(
		binaryPath, "kafka",
		"--brokers", "localhost:9092",
		"--topic", topicName,
		"--message", "Test existing topic",
		"--requestrate", "1",
		"--duration", "1s",
	)
	existingOutput, err := existingCmd.CombinedOutput()
	
	// Check results
	outputStr := string(existingOutput)
	require.NoError(t, err, "Failed when using existing topic: %s", outputStr)
	
	// Verify the output contains indication that topic already exists
	assert.True(t, 
		strings.Contains(outputStr, fmt.Sprintf("Topic '%s' already exists", topicName)) ||
		strings.Contains(outputStr, "Ensuring Kafka topic"),
		"Output should indicate topic exists: %s", outputStr)
}

// TestKafkaSCRAMTopicExists tests the scenario where a topic already exists with SCRAM authentication
func TestKafkaSCRAMTopicExists(t *testing.T) {
	// Skip this test if not running in CI or if KAFKA_TEST_ENABLED is not set
	if os.Getenv("KAFKA_TEST_ENABLED") != "true" {
		t.Skip("Skipping Kafka SCRAM test; set KAFKA_TEST_ENABLED=true to enable")
	}

	// Build the binary if it doesn't exist
	binaryPath := filepath.Join("..", "load-generator")
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		cmd := exec.Command("go", "build", "-o", binaryPath, "../main.go")
		err := cmd.Run()
		require.NoError(t, err, "Failed to build binary")
	}

	// Start Kafka SCRAM using docker compose
	startKafka := exec.Command("docker", "compose", "up", "-d", "zookeeper", "kafka-scram")
	err := startKafka.Run()
	require.NoError(t, err, "Failed to start Kafka SCRAM")

	// Ensure Kafka is cleaned up after the test
	defer func() {
		stopKafka := exec.Command("docker", "compose", "down")
		_ = stopKafka.Run()
	}()

	// Wait for Kafka SCRAM to start (longer wait time for SCRAM setup)
	time.Sleep(20 * time.Second)

	// Define topic name for this test
	topicName := "test-scram-existing-topic"

	// First run: Create the topic with SCRAM auth
	createCmd := exec.Command(
		binaryPath, "kafka",
		"--brokers", "localhost:9093",
		"--topic", topicName,
		"--message", "Create topic with SCRAM",
		"--username", "admin",
		"--password", "admin-secret",
		"--sasl-mechanism", "SCRAM-SHA-512",
		"--requestrate", "1",
		"--duration", "1s",
	)
	createOutput, err := createCmd.CombinedOutput()
	require.NoError(t, err, "Failed to create topic with SCRAM: %s", string(createOutput))

	// Wait for topic creation to complete (longer wait for SCRAM)
	time.Sleep(5 * time.Second)

	// Second run: Use the same topic to test existing topic handling with SCRAM
	existingCmd := exec.Command(
		binaryPath, "kafka",
		"--brokers", "localhost:9093",
		"--topic", topicName,
		"--message", "Test existing topic with SCRAM",
		"--username", "admin",
		"--password", "admin-secret",
		"--sasl-mechanism", "SCRAM-SHA-512",
		"--requestrate", "1",
		"--duration", "1s",
	)
	existingOutput, err := existingCmd.CombinedOutput()
	
	// Check results
	outputStr := string(existingOutput)
	require.NoError(t, err, "Failed when using existing topic with SCRAM: %s", outputStr)
	
	// Verify the output contains indication that topic already exists
	assert.True(t, 
		strings.Contains(outputStr, fmt.Sprintf("Topic '%s' already exists", topicName)) ||
		strings.Contains(outputStr, "Ensuring Kafka topic"),
		"Output should indicate topic exists: %s", outputStr)
}
