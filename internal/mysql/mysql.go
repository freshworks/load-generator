package mysql

// Hooks satisfies the sqlhook.Hooks interface
import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/go-sql-driver/mysql"
	"github.com/qustavo/sqlhooks/v2"
	"github.com/sirupsen/logrus"
)

// TODO: Find a way to make it not global!
var gStats *stats.Stats
var gStatsMux sync.Mutex

func init() {
	sql.Register("mysqlWithHooks", sqlhooks.Wrap(&mysql.MySQLDriver{}, &Hooks{}))
}

type Generator struct {
	DB *sql.DB

	log   *logrus.Entry
	o     GeneratorOptions
	ctx   context.Context
	stats *stats.Stats
}

type GeneratorOptions struct {
	Target string
	Query  string
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{
		Target: "root@tcp(127.0.0.1:3306)/",
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
	g.DB, err = g.Open(g.o.Target)
	if err != nil {
		return err
	}

	return g.DB.Ping()
}

func (g *Generator) InitDone() error {
	return nil
}

func (g *Generator) Tick() error {
	res, err := g.DB.QueryContext(g.ctx, g.o.Query)
	if err != nil {
		g.log.Errorf("MySQL error: %v", err)
		return nil
	}

	return res.Close()
}

func (g *Generator) Finish() error {
	if g.DB != nil {
		return g.DB.Close()
	}

	return nil
}

func (g *Generator) Open(dsn string) (*sql.DB, error) {
	return sql.Open("mysqlWithHooks", dsn)
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
