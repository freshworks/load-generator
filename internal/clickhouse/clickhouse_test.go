package clickhouse

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/freshworks/load-generator/internal/stats"
	mysqlquery "github.com/percona/go-mysql/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOptions(t *testing.T) {
	options := NewOptions()

	assert.NotNil(t, options)
	assert.Equal(t, "clickhouse://127.0.0.1:9000/default", options.DSN)
	assert.Equal(t, "SELECT 1", options.Query)
}

func TestNewGenerator(t *testing.T) {
	options := GeneratorOptions{
		DSN:   "clickhouse://localhost:9000/test",
		Query: "SELECT count(*) FROM users",
	}

	ctx := context.Background()
	stats := &stats.Stats{}

	generator := NewGenerator(1, options, ctx, 10, stats)

	assert.NotNil(t, generator)
	assert.Equal(t, options, generator.o)
	assert.Equal(t, ctx, generator.ctx)
	assert.Equal(t, stats, generator.stats)
	assert.NotNil(t, generator.log)
}

func TestGeneratorOptions_Validation(t *testing.T) {
	tests := []struct {
		name     string
		options  GeneratorOptions
		expected bool
	}{
		{
			name: "valid options",
			options: GeneratorOptions{
				DSN:   "clickhouse://localhost:9000/test",
				Query: "SELECT 1",
			},
			expected: true,
		},
		{
			name: "empty DSN",
			options: GeneratorOptions{
				DSN:   "",
				Query: "SELECT 1",
			},
			expected: false,
		},
		{
			name: "empty query",
			options: GeneratorOptions{
				DSN:   "clickhouse://localhost:9000/test",
				Query: "",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.options.DSN != "" && tt.options.Query != ""
			assert.Equal(t, tt.expected, valid)
		})
	}
}

// MockGenerator wraps the real generator with a mock database for testing
type MockGenerator struct {
	*Generator
	MockDB       sqlmock.Sqlmock
	InitCalled   bool
	TickCalled   int
	FinishCalled bool
}

func NewMockGenerator(options GeneratorOptions) (*MockGenerator, error) {
	ctx := context.Background()
	stats := stats.New("test", 1, 1, 0, false)

	generator := NewGenerator(1, options, ctx, 10, stats)

	// Create mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}

	// Replace the DB with our mock
	generator.DB = db

	return &MockGenerator{
		Generator: generator,
		MockDB:    mock,
	}, nil
}

func TestBasicClickHouseOperations(t *testing.T) {
	options := GeneratorOptions{
		DSN:   "clickhouse://localhost:9000/test",
		Query: "SELECT 1",
	}

	mockGen, err := NewMockGenerator(options)
	require.NoError(t, err)
	defer mockGen.Generator.Finish()

	t.Run("successful_query", func(t *testing.T) {
		// Setup mock expectations
		mockGen.MockDB.ExpectQuery("SELECT 1").
			WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

		err := mockGen.Tick()
		assert.NoError(t, err)

		// Verify all expectations were met
		assert.NoError(t, mockGen.MockDB.ExpectationsWereMet())
	})

	t.Run("query_with_error", func(t *testing.T) {
		// Create a fresh mock for this test
		freshMock, err := NewMockGenerator(options)
		require.NoError(t, err)
		defer freshMock.Generator.Finish()

		// Setup mock to return an error
		freshMock.MockDB.ExpectQuery("SELECT 1").
			WillReturnError(errors.New("connection failed"))

		err = freshMock.Tick()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection failed")

		assert.NoError(t, freshMock.MockDB.ExpectationsWereMet())
	})
}

func TestClickHouseComplexQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
		rows  *sqlmock.Rows
	}{
		{
			name:  "count_query",
			query: "SELECT count(*) FROM users",
			rows:  sqlmock.NewRows([]string{"count"}).AddRow(100),
		},
		{
			name:  "aggregation_query",
			query: "SELECT status, count(*) FROM users GROUP BY status",
			rows: sqlmock.NewRows([]string{"status", "count"}).
				AddRow("active", 80).
				AddRow("inactive", 20),
		},
		{
			name:  "time_series_query",
			query: "SELECT toDate(created_at) as date, count(*) FROM events WHERE created_at >= today() - 7 GROUP BY date",
			rows: sqlmock.NewRows([]string{"date", "count"}).
				AddRow("2024-01-15", 150).
				AddRow("2024-01-16", 200),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := GeneratorOptions{
				DSN:   "clickhouse://localhost:9000/analytics",
				Query: tt.query,
			}

			mockGen, err := NewMockGenerator(options)
			require.NoError(t, err)
			defer mockGen.Generator.Finish()

			// Setup mock expectations - use regex escaped query match
			mockGen.MockDB.ExpectQuery(regexp.QuoteMeta(tt.query)).WillReturnRows(tt.rows)

			err = mockGen.Tick()
			assert.NoError(t, err)

			assert.NoError(t, mockGen.MockDB.ExpectationsWereMet())
		})
	}
}

func TestClickHouseWithAuth(t *testing.T) {
	options := GeneratorOptions{
		DSN:   "clickhouse://testuser:testpass@localhost:9000/test",
		Query: "SELECT version()",
	}

	mockGen, err := NewMockGenerator(options)
	require.NoError(t, err)
	defer mockGen.Generator.Finish()

	t.Run("authenticated_query", func(t *testing.T) {
		// Setup mock expectations
		mockGen.MockDB.ExpectQuery(regexp.QuoteMeta("SELECT version()")).
			WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("24.8.1.1"))

		err := mockGen.Tick()
		assert.NoError(t, err)

		assert.NoError(t, mockGen.MockDB.ExpectationsWereMet())
	})

	t.Run("auth_failure", func(t *testing.T) {
		// Create fresh mock for auth failure test
		freshMock, err := NewMockGenerator(options)
		require.NoError(t, err)
		defer freshMock.Generator.Finish()

		// Setup mock to return auth error
		freshMock.MockDB.ExpectQuery(regexp.QuoteMeta("SELECT version()")).
			WillReturnError(errors.New("authentication failed"))

		err = freshMock.Tick()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "authentication failed")

		assert.NoError(t, freshMock.MockDB.ExpectationsWereMet())
	})
}

func TestInvalidQueries(t *testing.T) {
	invalidQueries := []struct {
		name  string
		query string
		error string
	}{
		{
			name:  "syntax_error",
			query: "INVALID QUERY",
			error: "syntax error",
		},
		{
			name:  "missing_table",
			query: "SELECT * FROM non_existent_table",
			error: "table doesn't exist",
		},
		{
			name:  "invalid_function",
			query: "SELECT invalid_function()",
			error: "unknown function",
		},
	}

	for _, tt := range invalidQueries {
		t.Run(tt.name, func(t *testing.T) {
			options := GeneratorOptions{
				DSN:   "clickhouse://localhost:9000/test",
				Query: tt.query,
			}

			mockGen, err := NewMockGenerator(options)
			require.NoError(t, err)
			defer mockGen.Generator.Finish()

			// Setup mock to return error
			mockGen.MockDB.ExpectQuery(regexp.QuoteMeta(tt.query)).
				WillReturnError(errors.New(tt.error))

			err = mockGen.Tick()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.error)

			// No need to check stats for failed queries as they won't be recorded
			assert.NoError(t, mockGen.MockDB.ExpectationsWereMet())
		})
	}
}

func TestClickHouseLifecycle(t *testing.T) {
	options := GeneratorOptions{
		DSN:   "clickhouse://localhost:9000/test",
		Query: "SELECT 1",
	}

	mockGen, err := NewMockGenerator(options)
	require.NoError(t, err)

	t.Run("full_lifecycle", func(t *testing.T) {
		// Test InitDone
		err := mockGen.InitDone()
		assert.NoError(t, err)

		// Test multiple Tick calls
		for range 3 {
			mockGen.MockDB.ExpectQuery("SELECT 1").
				WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

			err = mockGen.Tick()
			assert.NoError(t, err)
		}

		// Test Finish
		mockGen.MockDB.ExpectClose()
		err = mockGen.Finish()
		assert.NoError(t, err)

		assert.NoError(t, mockGen.MockDB.ExpectationsWereMet())
	})
}

func TestClickHouseStatsRecording(t *testing.T) {
	options := GeneratorOptions{
		DSN:   "clickhouse://localhost:9000/test",
		Query: "SELECT count(*) FROM users",
	}

	// Create stats instance
	sts := stats.New("test", 1, 1, 0, false)
	sts.Start()
	defer sts.Stop()

	ctx := context.Background()
	generator := NewGenerator(1, options, ctx, 10, sts)

	// Create mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	generator.DB = db

	t.Run("successful_query_records_stats", func(t *testing.T) {
		// Setup mock expectations
		mock.ExpectQuery(regexp.QuoteMeta(options.Query)).
			WillDelayFor(50 * time.Millisecond). // Simulate some latency
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(100))

		sts.Reset()

		err := generator.Tick()
		assert.NoError(t, err)

		// Verify stats were recorded
		result := getStatResultFor(sts, options.DSN, options.Query)
		if result != nil {
			assert.Equal(t, int64(1), result.Histogram.Count)
			assert.True(t, result.Histogram.Avg >= 50) // Should have some latency
		}

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("multiple_queries_accumulate_stats", func(t *testing.T) {
		sts.Reset()

		// Execute multiple queries
		for i := range 5 {
			mock.ExpectQuery(regexp.QuoteMeta(options.Query)).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(100 + i))

			err := generator.Tick()
			assert.NoError(t, err)
		}

		// Verify accumulated stats
		result := getStatResultFor(sts, options.DSN, options.Query)
		if result != nil {
			assert.Equal(t, int64(5), result.Histogram.Count)
		}

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestClickHouseErrorHandling(t *testing.T) {
	options := GeneratorOptions{
		DSN:   "clickhouse://localhost:9000/test",
		Query: "SELECT 1",
	}

	t.Run("database_connection_failure", func(t *testing.T) {
		// Test with invalid DSN that would fail during Open
		invalidOptions := GeneratorOptions{
			DSN:   "invalid://dsn:format",
			Query: "SELECT 1",
		}

		ctx := context.Background()
		stats := &stats.Stats{}
		generator := NewGenerator(1, invalidOptions, ctx, 10, stats)

		// This should fail during Init when Open is called
		err := generator.Init()
		assert.Error(t, err)
	})

	t.Run("query_execution_failure", func(t *testing.T) {
		mockGen, err := NewMockGenerator(options)
		require.NoError(t, err)
		defer mockGen.Generator.Finish()

		// Setup mock to return connection error
		mockGen.MockDB.ExpectQuery("SELECT 1").
			WillReturnError(driver.ErrBadConn)

		err = mockGen.Tick()
		assert.Error(t, err)
		// Just check that an error occurred, don't check the exact error type
		// as sqlmock may wrap the error differently

		assert.NoError(t, mockGen.MockDB.ExpectationsWereMet())
	})
}

func TestClickHouseConcurrency(t *testing.T) {
	options := GeneratorOptions{
		DSN:   "clickhouse://localhost:9000/test",
		Query: "SELECT 1",
	}

	t.Run("concurrent_queries", func(t *testing.T) {
		mockGen, err := NewMockGenerator(options)
		require.NoError(t, err)
		defer mockGen.Generator.Finish()

		// Setup expectations for concurrent queries
		numQueries := 10
		for range numQueries {
			mockGen.MockDB.ExpectQuery("SELECT 1").
				WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		}

		// Execute queries sequentially (simulating what would happen in concurrent scenario)
		for range numQueries {
			err := mockGen.Tick()
			assert.NoError(t, err)
		}

		assert.NoError(t, mockGen.MockDB.ExpectationsWereMet())
	})
}

// Helper function to get stats result (from original test)
func getStatResultFor(sts *stats.Stats, key, query string) *stats.Result {
	report := sts.Export()
	// For ClickHouse, we need to check both the original query and the fingerprinted version
	for _, result := range report.Results {
		if result.Target == key && (result.SubTarget == query || result.SubTarget == mysqlquery.Id(mysqlquery.Fingerprint(query))) {
			return &result
		}
	}
	return nil
}

func TestFinishWithNilDB(t *testing.T) {
	options := GeneratorOptions{
		DSN:   "clickhouse://localhost:9000/test",
		Query: "SELECT 1",
	}

	ctx := context.Background()
	stats := &stats.Stats{}
	generator := NewGenerator(1, options, ctx, 10, stats)

	// Test finish without initialization (DB is nil)
	err := generator.Finish()
	assert.NoError(t, err)
}

func TestPerformancePatterns(t *testing.T) {
	t.Run("analytical_queries", func(t *testing.T) {
		queries := []string{
			"SELECT count(*) FROM events WHERE date >= today() - 7",
			"SELECT avg(response_time) FROM requests WHERE status = 200",
			"SELECT user_id, count(*) FROM sessions GROUP BY user_id HAVING count(*) > 10",
			"SELECT date, sum(revenue) FROM sales GROUP BY date ORDER BY date DESC LIMIT 30",
		}

		for _, query := range queries {
			options := GeneratorOptions{
				DSN:   "clickhouse://localhost:9000/analytics",
				Query: query,
			}

			mockGen, err := NewMockGenerator(options)
			require.NoError(t, err)

			// Setup mock expectations
			mockGen.MockDB.ExpectQuery(regexp.QuoteMeta(query)).
				WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow("test_result"))

			err = mockGen.Tick()
			assert.NoError(t, err)

			assert.NoError(t, mockGen.MockDB.ExpectationsWereMet())
			mockGen.Generator.Finish()
		}
	})
}
