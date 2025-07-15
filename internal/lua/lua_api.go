package lua

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/freshworks/load-generator/internal/cql"
	"github.com/freshworks/load-generator/internal/grpc"
	"github.com/freshworks/load-generator/internal/http"
	"github.com/freshworks/load-generator/internal/mongo"
	"github.com/freshworks/load-generator/internal/mysql"
	"github.com/freshworks/load-generator/internal/psql"
	"github.com/freshworks/load-generator/internal/redis"
	"github.com/freshworks/load-generator/internal/smtp"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
	lua "github.com/yuin/gopher-lua"
	luar "layeh.com/gopher-luar"
)

// Shared across all workers
var (
	SharedMap              sync.Map
	markovChain            *MarkovChain
	tickDataMux            sync.Mutex
	tickDataFile           string
	tickFile               *os.File
	tickReader             interface{}
	loadTickDataInitOnce   sync.Once
	loadTickDataFinishOnce sync.Once
)

func init() {
	markovChain = NewMarkovChain(2)
	markovChain.Build()
}

type LG struct {
	Id          int
	Concurrency int
	Map         *sync.Map
	RequestRate int
	ScriptDir   string
	ScriptArgs  *lua.LTable

	log                    *logrus.Entry
	stats                  *stats.Stats
	ctx                    context.Context
	customMetricsCollector map[string]time.Time
}

func NewLG(id int, requestrate int, concurrency int, ctx context.Context, script string, s *stats.Stats, log *logrus.Entry) *LG {
	sd, _ := filepath.Abs(filepath.Dir(script))

	return &LG{
		Id:                     id,
		RequestRate:            requestrate,
		Concurrency:            concurrency,
		Map:                    &SharedMap,
		ScriptDir:              sd,
		ctx:                    ctx,
		customMetricsCollector: make(map[string]time.Time),
		stats:                  s,
		log:                    log,
	}
}

func (lg *LG) init() error {
	loadTickDataInitOnce.Do(func() {
		lg.initTickData()
	})

	return nil
}

func (lg *LG) finish() error {
	loadTickDataFinishOnce.Do(func() {
		lg.finishTickData()
	})

	return nil
}

func (lg *LG) preloadModules(L *lua.LState) {
	L.PreloadModule("grpc", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Options", luar.New(L,
			func() *grpc.GeneratorOptions {
				return grpc.NewOptions()
			}))
		mod.RawSetString("New", luar.New(L,
			func(o *grpc.GeneratorOptions) (*grpc.Generator, error) {
				g := grpc.NewGenerator(lg.Id, *o, lg.ctx, lg.stats, lg.RequestRate)
				return g, g.Init()
			}))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("http", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Options", luar.New(L,
			func() *http.GeneratorOptions {
				return http.NewOptions()
			}))
		mod.RawSetString("New", luar.New(L,
			func(o *http.GeneratorOptions) (*http.Generator, error) {
				g := http.NewGenerator(lg.Id, *o, lg.ctx, lg.RequestRate, lg.stats)
				return g, g.Init()
			}))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("redis", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Options", luar.New(L,
			func() *redis.GeneratorOptions {
				return redis.NewOptions()
			}))
		mod.RawSetString("New", luar.New(L,
			func(o *redis.GeneratorOptions) (*redis.Generator, error) {
				g := redis.NewGenerator(lg.Id, *o, lg.ctx, lg.RequestRate, lg.stats)
				return g, g.Init()
			}))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("mysql", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Options", luar.New(L,
			func() *mysql.GeneratorOptions {
				return mysql.NewOptions()
			}))
		mod.RawSetString("New", luar.New(L,
			func(o *mysql.GeneratorOptions) (*mysql.Generator, error) {
				g := mysql.NewGenerator(lg.Id, *o, lg.ctx, lg.RequestRate, lg.stats)
				return g, g.Init()
			}))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("cql", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Options", luar.New(L,
			func() *cql.GeneratorOptions {
				return cql.NewOptions()
			}))
		mod.RawSetString("New", luar.New(L,
			func(o *cql.GeneratorOptions) (*cql.Generator, error) {
				g := cql.NewGenerator(lg.Id, *o, lg.ctx, lg.RequestRate, lg.stats)
				return g, g.Init()
			}))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("psql", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Options", luar.New(L,
			func() *psql.GeneratorOptions {
				return psql.NewOptions()
			}))
		mod.RawSetString("New", luar.New(L,
			func(o *psql.GeneratorOptions) (*psql.Generator, error) {
				g := psql.NewGenerator(lg.Id, *o, lg.ctx, lg.RequestRate, lg.stats)
				return g, g.Init()
			}))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("smtp", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Options", luar.New(L,
			func() *smtp.GeneratorOptions {
				return smtp.NewOptions()
			}))
		mod.RawSetString("New", luar.New(L,
			func(o *smtp.GeneratorOptions) (*smtp.Generator, error) {
				g := smtp.NewGenerator(lg.Id, *o, lg.ctx, lg.RequestRate, lg.stats)
				return g, g.Init()
			}))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("mongo", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Options", luar.New(L,
			func() *mongo.GeneratorOptions {
				return mongo.NewOptions()
			}))
		mod.RawSetString("New", luar.New(L,
			func(o *mongo.GeneratorOptions) (*mongo.Generator, error) {
				g := mongo.NewGenerator(lg.Id, *o, lg.ctx, lg.RequestRate, lg.stats)
				return g, g.Init()
			}))
		L.Push(mod)
		return 1
	})
}

func (lg *LG) BeginCustomMetrics(keys ...string) {
	for _, v := range keys {
		lg.customMetricsCollector[v] = time.Now()
	}
}

func (lg *LG) RecordRawMetrics(key string, value int64) {
	ti := &stats.TraceInfo{
		Type:   stats.RawTrace,
		Key:    "raw",
		Subkey: key,
		Total:  time.Duration(value),
	}
	lg.stats.RecordMetric(ti)
}

func (lg *LG) EndCustomMetrics(keys ...string) error {
	return lg.prepareReportCustomMetric(false, keys...)
}

func (lg *LG) EndCustomMetricsWithError(keys ...string) error {
	return lg.prepareReportCustomMetric(true, keys...)
}

func (lg *LG) AbortCustomMetrics(keys ...string) {
	for _, v := range keys {
		delete(lg.customMetricsCollector, v)
	}
}

func (lg *LG) prepareReportCustomMetric(err bool, keys ...string) error {
	missing := ""
	for _, v := range keys {
		if s, ok := lg.customMetricsCollector[v]; ok {
			ti := &stats.TraceInfo{
				Type:   stats.CustomTrace,
				Key:    "custom",
				Subkey: v,
				Error:  err,
			}
			if !ti.Error {
				ti.Total = time.Since(s)
			}
			lg.stats.RecordMetric(ti)
			delete(lg.customMetricsCollector, v)
		} else {
			missing = missing + " " + v
		}
	}

	if missing != "" {
		return errors.New("Custom metrics keys missing: " + missing)
	}

	return nil
}

func (lg *LG) ShouldQuit() bool {
	select {
	case <-lg.ctx.Done():
		return true
	default:
		return false
	}
}

func (lg *LG) SetTickDataFile(f string) {
	tickDataMux.Lock()
	defer tickDataMux.Unlock()

	tickDataFile = f
}

func (lg *LG) finishTickData() {
	if tickFile != nil {
		tickFile.Close()
	}

	tickReader = nil
	tickFile = nil
}

func (lg *LG) initTickData() {
	if tickDataFile == "" {
		return
	}

	var err error
	tickFile, err = os.Open(tickDataFile)
	if err != nil {
		lg.log.Errorf("File (%v) open error: %v\n", tickDataFile, err)
		return
	}

	if filepath.Ext(tickDataFile) == ".csv" {
		tickReader = csv.NewReader(tickFile)
	} else {
		tickReader = bufio.NewReader(tickFile)
	}
}

func (lg *LG) getTickData() interface{} {
	if tickReader == nil {
		return nil
	}

	tickDataMux.Lock()
	defer tickDataMux.Unlock()

	iter := 0
	for iter <= 1 {
		iter++

		switch v := tickReader.(type) {
		case *csv.Reader:
			data, err := v.Read()
			if err != nil {
				if err != io.EOF {
					lg.log.Debugf("Error reading tick data file (%v): %v\n", tickDataFile, err)
				}
				tickFile.Seek(0, io.SeekStart)
				continue
			}
			return data
		case *bufio.Reader:
			line, err := v.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					lg.log.Debugf("Error reading tick data file (%v): %v\n", tickDataFile, err)
				}
				tickFile.Seek(0, io.SeekStart)
				v.Reset(tickFile)
				continue
			}
			return strings.TrimSuffix(line, "\n")
		default:
			lg.log.Fatalf("Unknown tick data file reader: %T", v)
		}
	}

	return nil
}

func (lg *LG) Context() context.Context {
	return lg.ctx
}
