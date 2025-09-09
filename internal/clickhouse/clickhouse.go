package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
)

// TODO: Find a way to make it not global!
var gStats *stats.Stats
var gStatsMux sync.Mutex

func init() {
	// Register ClickHouse driver with hooks
	// For now, we'll use a simpler approach without sqlhooks
	// TODO: Implement proper driver wrapping for ClickHouse
}

type Generator struct {
	DB *sql.DB

	log   *logrus.Entry
	o     GeneratorOptions
	ctx   context.Context
	stats *stats.Stats
}

type GeneratorOptions struct {
	DSN   string
	Query string
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{
		DSN:   "clickhouse://127.0.0.1:9000/default",
		Query: "SELECT 1",
	}
}

func NewGenerator(id int, options GeneratorOptions, ctx context.Context, requestrate int, s *stats.Stats) *Generator {
	gStatsMux.Lock()
	defer gStatsMux.Unlock()

	gStats = s

	log := logrus.WithFields(logrus.Fields{"Id": id})
	return &Generator{log: log, o: options, ctx: ctx, stats: s}
}

func (g *Generator) Init() error {
	var err error
	g.DB, err = g.Open(g.o.DSN)
	if err != nil {
		return err
	}

	return g.DB.Ping()
}

func (g *Generator) InitDone() error {
	return nil
}

func (g *Generator) Tick() error {
	start := time.Now()
	res, err := g.DB.QueryContext(g.ctx, g.o.Query)
	if err != nil {
		g.log.Errorf("ClickHouse error: %v", err)
		return err // Return the error so it's properly handled
	}

	err = res.Close()
	if err != nil {
		g.log.Errorf("ClickHouse close error: %v", err)
		return err
	}

	// Record successful query metrics
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.ClickHouseTrace
	traceInfo.Key = g.o.DSN // Use DSN as the key
	traceInfo.Subkey = g.o.Query
	traceInfo.Total = time.Since(start)
	g.stats.RecordMetric(&traceInfo)

	return nil
}

func (g *Generator) Finish() error {
	if g.DB != nil {
		return g.DB.Close()
	}

	return nil
}

func (g *Generator) Open(dsn string) (*sql.DB, error) {
	// Parse DSN and create ClickHouse connection
	opt, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return clickhouse.OpenDB(opt), nil
}

type Hooks struct{}
type begincontext string

var begin = begincontext("begin")

// Before hook will print the query with it's args and return the context with the timestamp
func (h *Hooks) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	return context.WithValue(ctx, begin, time.Now()), nil
}

// After hook will get the timestamp registered on the Before hook and print the elapsed time
func (h *Hooks) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.SqlTrace
	traceInfo.Key = "" // TODO: Set it host
	traceInfo.Subkey = query
	traceInfo.Total = time.Since(ctx.Value(begin).(time.Time))
	gStats.RecordMetric(&traceInfo)

	return ctx, nil
}

func (h *Hooks) OnError(ctx context.Context, err error, query string, args ...interface{}) error {
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.SqlTrace
	traceInfo.Key = "" // TODO: Set it host
	traceInfo.Subkey = query

	if !errors.Is(err, context.Canceled) {
		traceInfo.Error = true
	}
	gStats.RecordMetric(&traceInfo)

	return err
}
