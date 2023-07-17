package psql

import (
	"context"
	"database/sql"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/sirupsen/logrus"
)

type begincontext string
type querycontext string

var begin = begincontext("begin")
var query = querycontext("query")

func init() {
	sql.Register("psql", stdlib.GetDefaultDriver())
}

type tracer struct {
	stats *stats.Stats
}

func (t tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(
		context.WithValue(ctx, begin, time.Now()),
		query, data.SQL)
}

func (t tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.PGTrace
	traceInfo.Key = conn.Config().Host
	if data.Err == nil {
		traceInfo.Subkey = ctx.Value(query).(string)
		traceInfo.Total = time.Since(ctx.Value(begin).(time.Time))
	} else {
		traceInfo.Error = true
	}

	t.stats.RecordMetric(&traceInfo)

}

type Generator struct {
	DB    *sql.DB
	log   *logrus.Entry
	o     GeneratorOptions
	ctx   context.Context
	stats *stats.Stats
}

// https://pkg.go.dev/github.com/jackc/pgconn#Config
type GeneratorOptions struct {
	ConnectionString string
	Query            string
}

func NewGenerator(id int, options GeneratorOptions, ctx context.Context, requestrate int, s *stats.Stats) *Generator {

	log := logrus.WithFields(logrus.Fields{"Id": id})

	return &Generator{
		log:   log,
		o:     options,
		ctx:   ctx,
		stats: s,
	}
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{
		ConnectionString: "postgresql://postgres@127.0.0.1:5432/",
	}
}

func (g *Generator) Init() error {
	err := g.initClient()
	if err != nil {
		return err
	}
	return g.DB.Ping()
}

func (g *Generator) InitDone() error {
	return nil
}

func (g *Generator) initClient() error {

	var connConfig *pgx.ConnConfig
	var err error

	o := g.o
	connConfig, err = pgx.ParseConfig(o.ConnectionString)
	if nil != err {
		return err
	}

	connId := stdlib.RegisterConnConfig(connConfig)

	connConfig.Tracer = tracer{
		stats: g.stats,
	}

	db, err := sql.Open("psql", connId)
	if err != nil {
		return err
	}

	g.DB = db
	return nil
}

func (g *Generator) Tick() error {
	res, err := g.DB.QueryContext(g.ctx, g.o.Query)
	if err != nil {
		g.log.Errorf("PG error: %v", err)
	}
	if res != nil {
		return res.Close()
	}
	return nil
}

func (g *Generator) Finish() error {
	if g.DB != nil {
		return g.DB.Close()
	}

	return nil
}
