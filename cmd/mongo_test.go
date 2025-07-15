package cmd

import (
	"testing"
	"os/exec"
	"time"
	"strings"
	"os"
	"path/filepath"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMongoBasicOperations tests basic MongoDB operations
func TestMongoBasicOperations(t *testing.T) {
	// Skip this test if not running in CI or if MONGO_TEST_ENABLED is not set
	if os.Getenv("MONGO_TEST_ENABLED") != "true" {
		t.Skip("Skipping MongoDB test; set MONGO_TEST_ENABLED=true to enable")
	}

	// Build the binary if it doesn't exist
	binaryPath := filepath.Join("..", "lg")
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		cmd := exec.Command("go", "build", "-o", binaryPath, "../main.go")
		err := cmd.Run()
		require.NoError(t, err, "Failed to build binary")
	}

	// Start MongoDB using docker compose
	startMongo := exec.Command("docker", "compose", "up", "-d", "mongodb")
	err := startMongo.Run()
	require.NoError(t, err, "Failed to start MongoDB")

	// Ensure MongoDB is cleaned up after the test
	defer func() {
		stopMongo := exec.Command("docker", "compose", "down")
		_ = stopMongo.Run()
	}()

	// Wait for MongoDB to start
	time.Sleep(15 * time.Second)

	// Test 1: Insert operation
	insertCmd := exec.Command(
		binaryPath, "mongo",
		"--database", "testdb",
		"--collection", "users",
		"--operation", "insert",
		"--document", `{"name":"TestUser","email":"test@example.com","status":"active"}`,
		"--requestrate", "5",
		"--duration", "2s",
		"mongodb://admin:admin@localhost:27017",
	)
	insertOutput, err := insertCmd.CombinedOutput()
	require.NoError(t, err, "Failed to perform insert operation: %s", string(insertOutput))

	// Verify insert output contains expected metrics
	insertStr := string(insertOutput)
	assert.Contains(t, insertStr, "insert", "Output should contain insert operation metrics")
	assert.Contains(t, insertStr, "testdb.users", "Output should contain database.collection info")

	// Test 2: Find operation
	findCmd := exec.Command(
		binaryPath, "mongo",
		"--database", "testdb",
		"--collection", "users",
		"--operation", "find",
		"--filter", `{"status":"active"}`,
		"--requestrate", "10",
		"--duration", "2s",
		"mongodb://admin:admin@localhost:27017",
	)
	findOutput, err := findCmd.CombinedOutput()
	require.NoError(t, err, "Failed to perform find operation: %s", string(findOutput))

	// Verify find output contains expected metrics
	findStr := string(findOutput)
	assert.Contains(t, findStr, "find", "Output should contain find operation metrics")
	assert.Contains(t, findStr, "testdb.users", "Output should contain database.collection info")
}

// TestMongoUpdateAndDelete tests MongoDB update and delete operations
func TestMongoUpdateAndDelete(t *testing.T) {
	// Skip this test if not running in CI or if MONGO_TEST_ENABLED is not set
	if os.Getenv("MONGO_TEST_ENABLED") != "true" {
		t.Skip("Skipping MongoDB test; set MONGO_TEST_ENABLED=true to enable")
	}

	// Build the binary if it doesn't exist
	binaryPath := filepath.Join("..", "lg")
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		cmd := exec.Command("go", "build", "-o", binaryPath, "../main.go")
		err := cmd.Run()
		require.NoError(t, err, "Failed to build binary")
	}

	// Start MongoDB using docker compose
	startMongo := exec.Command("docker", "compose", "up", "-d", "mongodb")
	err := startMongo.Run()
	require.NoError(t, err, "Failed to start MongoDB")

	// Ensure MongoDB is cleaned up after the test
	defer func() {
		stopMongo := exec.Command("docker", "compose", "down")
		_ = stopMongo.Run()
	}()

	// Wait for MongoDB to start
	time.Sleep(15 * time.Second)

	// Test 1: Update operation
	updateCmd := exec.Command(
		binaryPath, "mongo",
		"--database", "testdb",
		"--collection", "users",
		"--operation", "update",
		"--filter", `{"status":"active"}`,
		"--update", `{"$set":{"last_updated":"2024-01-15"}}`,
		"--requestrate", "3",
		"--duration", "2s",
		"mongodb://admin:admin@localhost:27017",
	)
	updateOutput, err := updateCmd.CombinedOutput()
	require.NoError(t, err, "Failed to perform update operation: %s", string(updateOutput))

	// Verify update output contains expected metrics
	updateStr := string(updateOutput)
	assert.Contains(t, updateStr, "update", "Output should contain update operation metrics")
	assert.Contains(t, updateStr, "testdb.users", "Output should contain database.collection info")

	// Test 2: Delete operation
	deleteCmd := exec.Command(
		binaryPath, "mongo",
		"--database", "testdb",
		"--collection", "users",
		"--operation", "delete",
		"--filter", `{"status":"inactive"}`,
		"--requestrate", "2",
		"--duration", "2s",
		"mongodb://admin:admin@localhost:27017",
	)
	deleteOutput, err := deleteCmd.CombinedOutput()
	require.NoError(t, err, "Failed to perform delete operation: %s", string(deleteOutput))

	// Verify delete output contains expected metrics
	deleteStr := string(deleteOutput)
	assert.Contains(t, deleteStr, "delete", "Output should contain delete operation metrics")
	assert.Contains(t, deleteStr, "testdb.users", "Output should contain database.collection info")
}

// TestMongoAggregate tests MongoDB aggregation operations
func TestMongoAggregate(t *testing.T) {
	// Skip this test if not running in CI or if MONGO_TEST_ENABLED is not set
	if os.Getenv("MONGO_TEST_ENABLED") != "true" {
		t.Skip("Skipping MongoDB test; set MONGO_TEST_ENABLED=true to enable")
	}

	// Build the binary if it doesn't exist
	binaryPath := filepath.Join("..", "lg")
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		cmd := exec.Command("go", "build", "-o", binaryPath, "../main.go")
		err := cmd.Run()
		require.NoError(t, err, "Failed to build binary")
	}

	// Start MongoDB using docker compose
	startMongo := exec.Command("docker", "compose", "up", "-d", "mongodb")
	err := startMongo.Run()
	require.NoError(t, err, "Failed to start MongoDB")

	// Ensure MongoDB is cleaned up after the test
	defer func() {
		stopMongo := exec.Command("docker", "compose", "down")
		_ = stopMongo.Run()
	}()

	// Wait for MongoDB to start
	time.Sleep(15 * time.Second)

	// Test: Aggregate operation
	aggregateCmd := exec.Command(
		binaryPath, "mongo",
		"--database", "testdb",
		"--collection", "users",
		"--operation", "aggregate",
		"--filter", `[{"$match":{"status":"active"}},{"$group":{"_id":"$status","count":{"$sum":1}}}]`,
		"--requestrate", "2",
		"--duration", "2s",
		"mongodb://admin:admin@localhost:27017",
	)
	aggregateOutput, err := aggregateCmd.CombinedOutput()
	require.NoError(t, err, "Failed to perform aggregate operation: %s", string(aggregateOutput))

	// Verify aggregate output contains expected metrics
	aggregateStr := string(aggregateOutput)
	assert.Contains(t, aggregateStr, "aggregate", "Output should contain aggregate operation metrics")
	assert.Contains(t, aggregateStr, "testdb.users", "Output should contain database.collection info")
}

// TestMongoConnectionError tests MongoDB connection error handling
func TestMongoConnectionError(t *testing.T) {
	// Build the binary if it doesn't exist
	// Skip this test if not running in CI or if MONGO_TEST_ENABLED is not set
	if os.Getenv("MONGO_TEST_ENABLED") != "true" {
		t.Skip("Skipping MongoDB test; set MONGO_TEST_ENABLED=true to enable")
	}
	binaryPath := filepath.Join("..", "lg")
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		cmd := exec.Command("go", "build", "-o", binaryPath, "../main.go")
		err := cmd.Run()
		require.NoError(t, err, "Failed to build binary")
	}

	// Test connection to non-existent MongoDB instance
	errorCmd := exec.Command(
		binaryPath, "mongo",
		"--database", "testdb",
		"--collection", "users",
		"--operation", "find",
		"--filter", `{"status":"active"}`,
		"--requestrate", "1",
		"--duration", "1s",
		"--warmup", "1s", // Reduce warmup time
		"mongodb://localhost:27999", // Non-existent port
	)
	errorOutput, _ := errorCmd.CombinedOutput()
	
	// Print the actual output for debugging
	t.Logf("Error command output: %s", string(errorOutput))
	
	// The command may not return a non-zero exit code, but should log the error
	errorStr := string(errorOutput)
	
	// Verify error output contains connection error information
	assert.True(t, 
		strings.Contains(errorStr, "connection refused") || 
		strings.Contains(errorStr, "server selection timeout") ||
		strings.Contains(errorStr, "failed to ping MongoDB") ||
		strings.Contains(errorStr, "Initialization failed"),
		"Output should contain connection error info: %s", errorStr)
}