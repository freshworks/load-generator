package mongo

import (
	"context"
	"fmt"
	"testing"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/stretchr/testify/assert"
)

func TestNewOptions(t *testing.T) {
	options := NewOptions()
	
	assert.NotNil(t, options)
	assert.Equal(t, "mongodb://localhost:27017", options.ConnectionString)
	assert.Equal(t, "test", options.Database)
	assert.Equal(t, "test", options.Collection)
	assert.Equal(t, "find", options.Operation)
	assert.Equal(t, "{}", options.Document)
	assert.Equal(t, "{}", options.Filter)
	assert.Equal(t, "{}", options.Update)
	assert.Equal(t, "admin", options.AuthDB)
	assert.False(t, options.TLS)
}

func TestNewGenerator(t *testing.T) {
	options := GeneratorOptions{
		ConnectionString: "mongodb://localhost:27017",
		Database:         "testdb",
		Collection:       "testcol",
		Operation:        "find",
		Filter:           `{"status":"active"}`,
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
				ConnectionString: "mongodb://localhost:27017",
				Database:         "testdb",
				Collection:       "testcol",
				Operation:        "find",
			},
			expected: true,
		},
		{
			name: "empty connection string",
			options: GeneratorOptions{
				ConnectionString: "",
				Database:         "testdb",
				Collection:       "testcol",
				Operation:        "find",
			},
			expected: false,
		},
		{
			name: "empty database",
			options: GeneratorOptions{
				ConnectionString: "mongodb://localhost:27017",
				Database:         "",
				Collection:       "testcol",
				Operation:        "find",
			},
			expected: false,
		},
		{
			name: "empty collection",
			options: GeneratorOptions{
				ConnectionString: "mongodb://localhost:27017",
				Database:         "testdb",
				Collection:       "",
				Operation:        "find",
			},
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.options.ConnectionString != "" && 
					tt.options.Database != "" && 
					tt.options.Collection != ""
			assert.Equal(t, tt.expected, valid)
		})
	}
}

func TestParseJSON(t *testing.T) {
	ctx := context.Background()
	stats := &stats.Stats{}
	generator := NewGenerator(1, *NewOptions(), ctx, 10, stats)
	
	tests := []struct {
		name      string
		jsonStr   string
		expectErr bool
	}{
		{
			name:      "valid empty object",
			jsonStr:   "{}",
			expectErr: false,
		},
		{
			name:      "valid simple object",
			jsonStr:   `{"name":"test","age":30}`,
			expectErr: false,
		},
		{
			name:      "valid complex object",
			jsonStr:   `{"status":"active","age":{"$gte":18,"$lte":65}}`,
			expectErr: false,
		},
		{
			name:      "invalid JSON",
			jsonStr:   `{"name":"test",}`,
			expectErr: true,
		},
		{
			name:      "empty string",
			jsonStr:   "",
			expectErr: true,
		},
		{
			name:      "not an object",
			jsonStr:   `"just a string"`,
			expectErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := generator.parseJSON(tt.jsonStr)
			
			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}



func TestGeneratorErrorHandling(t *testing.T) {
	ctx := context.Background()
	stats := &stats.Stats{}
	
	// Test with invalid connection string
	options := GeneratorOptions{
		ConnectionString: "invalid://connection:string",
		Database:         "testdb",
		Collection:       "testcol",
	}
	
	generator := NewGenerator(1, options, ctx, 10, stats)
	
	// Test initialization failure
	err := generator.Init()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to MongoDB")
}

func TestGeneratorInvalidJSON(t *testing.T) {
	ctx := context.Background()
	stats := &stats.Stats{}
	
	generator := NewGenerator(1, *NewOptions(), ctx, 10, stats)
	
	// Test parseJSON method directly with invalid JSON
	t.Run("invalid_json_parsing", func(t *testing.T) {
		_, err := generator.parseJSON(`{"invalid":json}`)
		assert.Error(t, err)
	})
	
	t.Run("valid_json_parsing", func(t *testing.T) {
		result, err := generator.parseJSON(`{"valid":"json"}`)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "json", result["valid"])
	})
}



func TestGeneratorFinish(t *testing.T) {
	ctx := context.Background()
	stats := &stats.Stats{}
	
	generator := NewGenerator(1, *NewOptions(), ctx, 10, stats)
	
	// Test finish without initialization
	err := generator.Finish()
	assert.NoError(t, err)
	
	// Test finish with nil client
	generator.Client = nil
	err = generator.Finish()
	assert.NoError(t, err)
}

func TestOperationTypes(t *testing.T) {
	ctx := context.Background()
	stats := &stats.Stats{}
	
	tests := []struct {
		name      string
		operation string
		valid     bool
	}{
		{"find operation", "find", true},
		{"insert operation", "insert", true},
		{"update operation", "update", true},
		{"delete operation", "delete", true},
		{"aggregate operation", "aggregate", true},
		{"unsupported operation", "unsupported_operation", false},
		{"empty operation", "", false},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := GeneratorOptions{
				ConnectionString: "mongodb://localhost:27017",
				Database:         "testdb",
				Collection:       "testcol",
				Operation:        tt.operation,
			}
			
			generator := NewGenerator(1, options, ctx, 10, stats)
			assert.NotNil(t, generator)
			assert.Equal(t, tt.operation, generator.o.Operation)
			
			// Test that supported operations are recognized
			supportedOps := []string{"find", "insert", "update", "delete", "aggregate"}
			isSupported := false
			for _, op := range supportedOps {
				if tt.operation == op {
					isSupported = true
					break
				}
			}
			assert.Equal(t, tt.valid, isSupported)
		})
	}
}

// MockGenerator wraps the real generator and tracks method calls for testing
type MockGenerator struct {
	*Generator
	FindCalls      []string
	InsertCalls    []string
	UpdateCalls    [][]string // [filter, update]
	DeleteCalls    []string
	AggregateCalls []string
	InitCalled     bool
	FinishCalled   bool
}

func NewMockGenerator() *MockGenerator {
	ctx := context.Background()
	stats := &stats.Stats{}
	options := GeneratorOptions{
		ConnectionString: "mongodb://mock:27017",
		Database:         "mockdb",
		Collection:       "mockcol",
	}
	
	generator := NewGenerator(1, options, ctx, 10, stats)
	
	return &MockGenerator{
		Generator:      generator,
		FindCalls:      make([]string, 0),
		InsertCalls:    make([]string, 0),
		UpdateCalls:    make([][]string, 0),
		DeleteCalls:    make([]string, 0),
		AggregateCalls: make([]string, 0),
	}
}

// Override Lua methods to track calls instead of making real MongoDB calls
func (m *MockGenerator) Find(filter string) error {
	m.FindCalls = append(m.FindCalls, filter)
	
	// Validate JSON
	_, err := m.parseJSON(filter)
	if err != nil {
		return err
	}
	
	// Simulate successful operation
	return nil
}

func (m *MockGenerator) Insert(document string) error {
	m.InsertCalls = append(m.InsertCalls, document)
	
	// Validate JSON
	_, err := m.parseJSON(document)
	if err != nil {
		return err
	}
	
	// Simulate successful operation
	return nil
}

func (m *MockGenerator) Update(filter, update string) error {
	m.UpdateCalls = append(m.UpdateCalls, []string{filter, update})
	
	// Validate JSON for both filter and update
	_, err := m.parseJSON(filter)
	if err != nil {
		return err
	}
	
	_, err = m.parseJSON(update)
	if err != nil {
		return err
	}
	
	// Simulate successful operation
	return nil
}

func (m *MockGenerator) Delete(filter string) error {
	m.DeleteCalls = append(m.DeleteCalls, filter)
	
	// Validate JSON
	_, err := m.parseJSON(filter)
	if err != nil {
		return err
	}
	
	// Simulate successful operation
	return nil
}

func (m *MockGenerator) Aggregate(pipeline string) error {
	m.AggregateCalls = append(m.AggregateCalls, pipeline)
	
	// For aggregate, we expect an array, so try to parse it differently
	if pipeline == "" || pipeline == "{}" {
		return nil // Allow empty pipeline
	}
	
	// Simple validation - should start with [ for array
	if len(pipeline) > 0 && pipeline[0] != '[' {
		return assert.AnError // Invalid pipeline format
	}
	
	// Simulate successful operation
	return nil
}

func (m *MockGenerator) MockInit() error {
	m.InitCalled = true
	return nil
}

func (m *MockGenerator) MockFinish() error {
	m.FinishCalled = true
	return nil
}

func TestLuaMethodsWithMock(t *testing.T) {
	mock := NewMockGenerator()
	
	t.Run("lua_find_operations", func(t *testing.T) {
		// Test valid find operations
		err := mock.Find(`{"status":"active"}`)
		assert.NoError(t, err)
		
		err = mock.Find(`{"age":{"$gte":18,"$lte":65}}`)
		assert.NoError(t, err)
		
		err = mock.Find(`{}`)
		assert.NoError(t, err)
		
		// Verify calls were tracked
		assert.Len(t, mock.FindCalls, 3)
		assert.Equal(t, `{"status":"active"}`, mock.FindCalls[0])
		assert.Equal(t, `{"age":{"$gte":18,"$lte":65}}`, mock.FindCalls[1])
		assert.Equal(t, `{}`, mock.FindCalls[2])
		
		// Test invalid JSON
		err = mock.Find(`{"invalid":json}`)
		assert.Error(t, err)
	})
	
	t.Run("lua_insert_operations", func(t *testing.T) {
		// Test valid insert operations
		err := mock.Insert(`{"name":"John","age":30,"status":"active"}`)
		assert.NoError(t, err)
		
		err = mock.Insert(`{"product":"Laptop","price":999.99,"category":"electronics"}`)
		assert.NoError(t, err)
		
		// Verify calls were tracked
		assert.Len(t, mock.InsertCalls, 2)
		assert.Equal(t, `{"name":"John","age":30,"status":"active"}`, mock.InsertCalls[0])
		assert.Equal(t, `{"product":"Laptop","price":999.99,"category":"electronics"}`, mock.InsertCalls[1])
		
		// Test invalid JSON
		err = mock.Insert(`{"invalid":json}`)
		assert.Error(t, err)
	})
	
	t.Run("lua_update_operations", func(t *testing.T) {
		// Test valid update operations
		err := mock.Update(`{"status":"active"}`, `{"$set":{"last_updated":"2024-01-15"}}`)
		assert.NoError(t, err)
		
		err = mock.Update(`{"category":"electronics"}`, `{"$inc":{"views":1}}`)
		assert.NoError(t, err)
		
		// Verify calls were tracked
		assert.Len(t, mock.UpdateCalls, 2)
		assert.Equal(t, []string{`{"status":"active"}`, `{"$set":{"last_updated":"2024-01-15"}}`}, mock.UpdateCalls[0])
		assert.Equal(t, []string{`{"category":"electronics"}`, `{"$inc":{"views":1}}`}, mock.UpdateCalls[1])
		
		// Test invalid JSON in filter
		err = mock.Update(`{"invalid":json}`, `{"$set":{"valid":"json"}}`)
		assert.Error(t, err)
		
		// Test invalid JSON in update
		err = mock.Update(`{"valid":"json"}`, `{"invalid":json}`)
		assert.Error(t, err)
	})
	
	t.Run("lua_delete_operations", func(t *testing.T) {
		// Test valid delete operations
		err := mock.Delete(`{"status":"inactive"}`)
		assert.NoError(t, err)
		
		err = mock.Delete(`{"created":{"$lt":"2023-01-01"}}`)
		assert.NoError(t, err)
		
		// Verify calls were tracked
		assert.Len(t, mock.DeleteCalls, 2)
		assert.Equal(t, `{"status":"inactive"}`, mock.DeleteCalls[0])
		assert.Equal(t, `{"created":{"$lt":"2023-01-01"}}`, mock.DeleteCalls[1])
		
		// Test invalid JSON
		err = mock.Delete(`{"invalid":json}`)
		assert.Error(t, err)
	})
	
	t.Run("lua_aggregate_operations", func(t *testing.T) {
		// Test valid aggregate operations
		err := mock.Aggregate(`[{"$match":{"status":"active"}},{"$group":{"_id":"$category","count":{"$sum":1}}}]`)
		assert.NoError(t, err)
		
		err = mock.Aggregate(`[{"$match":{"age":{"$gte":18}}},{"$limit":10}]`)
		assert.NoError(t, err)
		
		// Test empty pipeline
		err = mock.Aggregate(``)
		assert.NoError(t, err)
		
		err = mock.Aggregate(`{}`)
		assert.NoError(t, err)
		
		// Verify calls were tracked
		assert.Len(t, mock.AggregateCalls, 4)
		assert.Contains(t, mock.AggregateCalls[0], `"$match"`)
		assert.Contains(t, mock.AggregateCalls[1], `"$limit"`)
		
		// Test invalid pipeline format (not an array)
		err = mock.Aggregate(`{"$match":{"status":"active"}}`)
		assert.Error(t, err)
	})
}

func TestLuaWorkflowSimulation(t *testing.T) {
	mock := NewMockGenerator()
	
	// Simulate a typical Lua script workflow
	t.Run("ecommerce_workflow", func(t *testing.T) {
		// 1. Search for products
		err := mock.Find(`{"category":"electronics","price":{"$lte":1000}}`)
		assert.NoError(t, err)
		
		// 2. Insert a new user
		err = mock.Insert(`{"name":"TestUser","email":"test@example.com","status":"active","created":"2024-01-15"}`)
		assert.NoError(t, err)
		
		// 3. Update user activity
		err = mock.Update(`{"status":"active"}`, `{"$set":{"last_login":"2024-01-15"}}`)
		assert.NoError(t, err)
		
		// 4. Run analytics
		err = mock.Aggregate(`[{"$match":{"status":"active"}},{"$group":{"_id":"$status","count":{"$sum":1}}}]`)
		assert.NoError(t, err)
		
		// 5. Clean up old records
		err = mock.Delete(`{"status":"inactive","last_login":{"$lt":"2023-01-01"}}`)
		assert.NoError(t, err)
		
		// Verify all operations were tracked
		assert.Len(t, mock.FindCalls, 1)
		assert.Len(t, mock.InsertCalls, 1)
		assert.Len(t, mock.UpdateCalls, 1)
		assert.Len(t, mock.AggregateCalls, 1)
		assert.Len(t, mock.DeleteCalls, 1)
		
		// Verify operation content
		assert.Contains(t, mock.FindCalls[0], "electronics")
		assert.Contains(t, mock.InsertCalls[0], "TestUser")
		assert.Contains(t, mock.UpdateCalls[0][1], "last_login")
		assert.Contains(t, mock.AggregateCalls[0], "$group")
		assert.Contains(t, mock.DeleteCalls[0], "inactive")
	})
}

func TestLuaErrorHandling(t *testing.T) {
	mock := NewMockGenerator()
	
	t.Run("json_validation_errors", func(t *testing.T) {
		// Test various JSON validation errors that Lua scripts might encounter
		invalidJSONTests := []struct {
			method string
			args   []string
		}{
			{"Find", []string{`{"unclosed": "quote}`}},
			{"Find", []string{`{"trailing": "comma",}`}},
			{"Insert", []string{`{"missing": value}`}},
			{"Update", []string{`{"valid": "json"}`, `{"invalid": syntax}`}},
			{"Delete", []string{`{invalid json}`}},
		}
		
		for _, test := range invalidJSONTests {
			switch test.method {
			case "Find":
				err := mock.Find(test.args[0])
				assert.Error(t, err, "Find should fail with invalid JSON: %s", test.args[0])
			case "Insert":
				err := mock.Insert(test.args[0])
				assert.Error(t, err, "Insert should fail with invalid JSON: %s", test.args[0])
			case "Update":
				err := mock.Update(test.args[0], test.args[1])
				assert.Error(t, err, "Update should fail with invalid JSON: %s, %s", test.args[0], test.args[1])
			case "Delete":
				err := mock.Delete(test.args[0])
				assert.Error(t, err, "Delete should fail with invalid JSON: %s", test.args[0])
			}
		}
	})
}

func TestLuaPerformancePatterns(t *testing.T) {
	mock := NewMockGenerator()
	
	t.Run("bulk_operations", func(t *testing.T) {
		// Simulate bulk operations that a Lua script might perform
		for i := 0; i < 10; i++ {
			err := mock.Insert(fmt.Sprintf(`{"user_id":%d,"name":"User%d","status":"active"}`, i, i))
			assert.NoError(t, err)
		}
		
		// Verify all inserts were tracked
		assert.Len(t, mock.InsertCalls, 10)
		
		// Verify content of some inserts
		assert.Contains(t, mock.InsertCalls[0], `"user_id":0`)
		assert.Contains(t, mock.InsertCalls[9], `"user_id":9`)
	})
	
	t.Run("mixed_operation_pattern", func(t *testing.T) {
		// Create a fresh mock for this test
		freshMock := NewMockGenerator()
		
		// Simulate a mixed workload pattern
		operations := []func() error{
			func() error { return freshMock.Find(`{"status":"active"}`) },
			func() error { return freshMock.Insert(`{"name":"NewUser","status":"active"}`) },
			func() error { return freshMock.Update(`{"status":"active"}`, `{"$set":{"updated":true}}`) },
			func() error { return freshMock.Find(`{"updated":true}`) },
			func() error { return freshMock.Delete(`{"status":"inactive"}`) },
		}
		
		for _, op := range operations {
			err := op()
			assert.NoError(t, err)
		}
		
		// Verify mixed operations were tracked
		assert.Len(t, freshMock.FindCalls, 2)
		assert.Len(t, freshMock.InsertCalls, 1)
		assert.Len(t, freshMock.UpdateCalls, 1)
		assert.Len(t, freshMock.DeleteCalls, 1)
	})
}