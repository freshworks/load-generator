package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	redis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

type Generator struct {
	log *logrus.Entry

	Client *redis.Client
	cmd    *redis.StringCmd
	o      GeneratorOptions
	stats  *stats.Stats
	ctx    context.Context
}

type GeneratorOptions struct {
	Target   string
	Password string
	Username string
	Database int
	Cmd      string
	Args     []string
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{}
}

func NewGenerator(id int, options GeneratorOptions, ctx context.Context, requestrate int, s *stats.Stats) *Generator {
	log := logrus.WithFields(logrus.Fields{"Id": id})
	return &Generator{log: log, o: options, stats: s, ctx: ctx}
}

func (r *Generator) Init() error {
	r.Client = r.Open()
	if r.Client == nil {
		return fmt.Errorf("cannot get redis client")
	}

	s := r.Client.Ping(r.ctx)
	r.log.Debugf("Redis init: %v", s)
	if s.Err() != nil {
		return s.Err()
	}

	a := append([]string{r.o.Cmd}, r.o.Args...)

	args := make([]interface{}, len(a))
	for i, v := range a {
		args[i] = v
	}

	r.cmd = redis.NewStringCmd(r.ctx, args...)

	return nil
}

func (r *Generator) InitDone() error {
	return nil
}

func (r *Generator) Tick() error {
	r.Client.Process(r.ctx, r.cmd)
	v, err := r.cmd.Result()
	r.log.Debugf("res=%v err=%v", v, err)

	if err != nil {
		if err == redis.Nil {
			return nil
		}
		r.log.Errorf("Redis error: %v", err)
	}

	return nil
}

func (r *Generator) Finish() error {
	if r.Client != nil {
		return r.Client.Close()
	}

	return nil
}

func (r *Generator) Open() *redis.Client {
	cl := redis.NewClient(&redis.Options{
		Addr:     r.o.Target,
		Password: r.o.Password,
		Username: r.o.Username,
		DB:       r.o.Database,
	})

	cl.AddHook(&redisHook{gn: r})

	return cl
}

type redisHook struct {
	gn *Generator
}

type begincontext string

var begin = begincontext("begin")

func (rh *redisHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	// fmt.Printf("starting processing: <%s>\n", cmd)
	return context.WithValue(ctx, begin, time.Now()), nil
}

func (rh *redisHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	// fmt.Printf("finished processing: <%s>\n", cmd)
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.RedisTrace
	traceInfo.Key = rh.gn.o.Target
	traceInfo.Subkey = cmd.Name()

	err := cmd.Err()
	traceInfo.Total = time.Since(ctx.Value(begin).(time.Time))

	if err != nil && err != redis.Nil {
		traceInfo.Error = true
	}

	rh.gn.stats.RecordMetric(&traceInfo)
	return err
}

func (rh *redisHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, begin, time.Now()), nil
}

func (rh *redisHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	var err error
	// fmt.Printf("pipeline finished processing: %v\n", cmds)
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.RedisTrace
	traceInfo.Key = rh.gn.o.Target
	traceInfo.Subkey = "pipeline"

	for _, cmd := range cmds {
		if err = cmd.Err(); err != nil {
			traceInfo.Error = true
			break
		}
	}

	traceInfo.Total = time.Since(ctx.Value(begin).(time.Time))

	rh.gn.stats.RecordMetric(&traceInfo)
	return err
}
