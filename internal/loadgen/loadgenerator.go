package loadgen

import (
	"context"

	"github.com/freshworks/load-generator/internal/stats"
	log "github.com/sirupsen/logrus"
)

type Generator interface {
	Init() error
	InitDone() error
	Tick() error
	Finish() error
}

type NewGenerator func(id int, requestrate int, concurrency int, ctx context.Context, stat *stats.Stats) Generator

type LoadGenerator struct {
	workChan  chan interface{}
	log       *log.Entry
	generator Generator
	ctx       context.Context
}

func NewLoadGenerator(id int, requestrate int, concurrency int, newGenerator NewGenerator, workCh chan interface{}, ctx context.Context, s *stats.Stats) *LoadGenerator {
	return &LoadGenerator{
		workChan:  workCh,
		log:       log.WithFields(log.Fields{"Id": id}),
		ctx:       ctx,
		generator: newGenerator(id, requestrate, concurrency, ctx, s),
	}
}

func (lg *LoadGenerator) Init() error {
	lg.log.Debugf("Calling init for generator: %T", lg.generator)
	return lg.generator.Init()
}

func (lg *LoadGenerator) Run() {
	if err := lg.generator.InitDone(); err != nil {
		lg.log.Errorf("InitDone error: %v", err)
	}

	lg.log.Debugf("Starting run")

out:
	for {
		select {
		case <-lg.workChan:
			err := lg.generator.Tick()
			if err != nil {
				lg.log.Warnf("%v", err)
				break out
			}
		case <-lg.ctx.Done():
			break out
		}
	}

	lg.log.Debugf("Exiting run")
}
