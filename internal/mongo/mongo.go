package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Generator struct {
	Client     *mongo.Client
	Database   *mongo.Database
	Collection *mongo.Collection

	log   *logrus.Entry
	o     GeneratorOptions
	ctx   context.Context
	stats *stats.Stats
}

type GeneratorOptions struct {
	ConnectionString string
	Database         string
	Collection       string
	Operation        string
	Document         string
	Filter           string
	Update           string
	Username         string
	Password         string
	AuthDB           string
	TLS              bool
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{
		ConnectionString: "mongodb://localhost:27017",
		Database:         "test",
		Collection:       "test",
		Operation:        "find",
		Document:         "{}",
		Filter:           "{}",
		Update:           "{}",
		AuthDB:           "admin",
		TLS:              false,
	}
}

func NewGenerator(id int, options GeneratorOptions, ctx context.Context, requestrate int, s *stats.Stats) *Generator {
	log := logrus.WithFields(logrus.Fields{"Id": id})
	return &Generator{log: log, o: options, ctx: ctx, stats: s}
}

func (g *Generator) Init() error {
	clientOptions := options.Client().ApplyURI(g.o.ConnectionString)

	// Set authentication if provided
	if g.o.Username != "" && g.o.Password != "" {
		credential := options.Credential{
			Username:   g.o.Username,
			Password:   g.o.Password,
			AuthSource: g.o.AuthDB,
		}
		clientOptions.SetAuth(credential)
	}

	// Enable TLS if requested
	if g.o.TLS {
		clientOptions.SetTLSConfig(nil) // Use default TLS config
	}

	var err error
	g.Client, err = mongo.Connect(g.ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test the connection
	err = g.Client.Ping(g.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	g.Database = g.Client.Database(g.o.Database)
	g.Collection = g.Database.Collection(g.o.Collection)

	g.log.Infof("Connected to MongoDB: %s/%s.%s", g.o.ConnectionString, g.o.Database, g.o.Collection)
	return nil
}

func (g *Generator) InitDone() error {
	return nil
}

func (g *Generator) Tick() error {
	start := time.Now()
	var err error

	switch strings.ToLower(g.o.Operation) {
	case "find":
		err = g.performFind()
	case "insert":
		err = g.performInsert()
	case "update":
		err = g.performUpdate()
	case "delete":
		err = g.performDelete()
	case "aggregate":
		err = g.performAggregate()
	default:
		err = fmt.Errorf("unsupported operation: %s", g.o.Operation)
	}

	// Record metrics
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.MongoTrace
	traceInfo.Key = fmt.Sprintf("%s.%s", g.o.Database, g.o.Collection)
	traceInfo.Subkey = g.o.Operation
	traceInfo.Total = time.Since(start)
	
	if err != nil {
		// Don't count context cancellation as a real error (happens when test ends)
		if !errors.Is(err, context.Canceled) {
			traceInfo.Error = true
			g.log.Errorf("MongoDB %s error: %v", g.o.Operation, err)
		}
	}

	g.stats.RecordMetric(&traceInfo)
	return nil
}

func (g *Generator) performFind() error {
	filter, err := g.parseJSON(g.o.Filter)
	if err != nil {
		return fmt.Errorf("invalid filter JSON: %w", err)
	}

	cursor, err := g.Collection.Find(g.ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(g.ctx)

	// Consume the cursor to simulate real usage
	var results []bson.M
	return cursor.All(g.ctx, &results)
}

func (g *Generator) performInsert() error {
	document, err := g.parseJSON(g.o.Document)
	if err != nil {
		return fmt.Errorf("invalid document JSON: %w", err)
	}

	_, err = g.Collection.InsertOne(g.ctx, document)
	return err
}

func (g *Generator) performUpdate() error {
	filter, err := g.parseJSON(g.o.Filter)
	if err != nil {
		return fmt.Errorf("invalid filter JSON: %w", err)
	}

	update, err := g.parseJSON(g.o.Update)
	if err != nil {
		return fmt.Errorf("invalid update JSON: %w", err)
	}

	_, err = g.Collection.UpdateMany(g.ctx, filter, update)
	return err
}

func (g *Generator) performDelete() error {
	filter, err := g.parseJSON(g.o.Filter)
	if err != nil {
		return fmt.Errorf("invalid filter JSON: %w", err)
	}

	_, err = g.Collection.DeleteMany(g.ctx, filter)
	return err
}

func (g *Generator) performAggregate() error {
	// For aggregate, we'll use the filter as the pipeline
	var pipeline []bson.M
	if g.o.Filter != "{}" {
		var pipelineInterface interface{}
		err := json.Unmarshal([]byte(g.o.Filter), &pipelineInterface)
		if err != nil {
			return fmt.Errorf("invalid aggregate pipeline JSON: %w", err)
		}

		// Convert to []bson.M
		switch v := pipelineInterface.(type) {
		case []interface{}:
			for _, stage := range v {
				if stageMap, ok := stage.(map[string]interface{}); ok {
					pipeline = append(pipeline, stageMap)
				}
			}
		case map[string]interface{}:
			pipeline = []bson.M{v}
		default:
			return fmt.Errorf("aggregate pipeline must be an array or object")
		}
	} else {
		// Default pipeline - just match all documents
		pipeline = []bson.M{{"$match": bson.M{}}}
	}

	cursor, err := g.Collection.Aggregate(g.ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(g.ctx)

	// Consume the cursor
	var results []bson.M
	return cursor.All(g.ctx, &results)
}

func (g *Generator) parseJSON(jsonStr string) (bson.M, error) {
	var result bson.M
	err := json.Unmarshal([]byte(jsonStr), &result)
	return result, err
}

func (g *Generator) Finish() error {
	if g.Client != nil {
		return g.Client.Disconnect(g.ctx)
	}
	return nil
}

// Lua-specific methods for different MongoDB operations
func (g *Generator) Find(filter string) error {
	g.o.Operation = "find"
	g.o.Filter = filter
	err := g.performFind()
	// Don't return context cancellation errors to Lua
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (g *Generator) Insert(document string) error {
	g.o.Operation = "insert"
	g.o.Document = document
	err := g.performInsert()
	// Don't return context cancellation errors to Lua
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (g *Generator) Update(filter, update string) error {
	g.o.Operation = "update"
	g.o.Filter = filter
	g.o.Update = update
	err := g.performUpdate()
	// Don't return context cancellation errors to Lua
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (g *Generator) Delete(filter string) error {
	g.o.Operation = "delete"
	g.o.Filter = filter
	err := g.performDelete()
	// Don't return context cancellation errors to Lua
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (g *Generator) Aggregate(pipeline string) error {
	g.o.Operation = "aggregate"
	g.o.Filter = pipeline
	err := g.performAggregate()
	// Don't return context cancellation errors to Lua
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}