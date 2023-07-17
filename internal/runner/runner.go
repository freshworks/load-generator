package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/stats"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type Runner struct {
	requestrate  int
	concurrency  int
	warmup       time.Duration
	duration     time.Duration
	newGenerator loadgen.NewGenerator
	ctx          context.Context
	cancel       context.CancelFunc
	workChan     chan interface{}
	stats        *stats.Stats
}

func New(requestrate, concurrency int, warmup, duration time.Duration, ctx context.Context, s *stats.Stats, newGenerator loadgen.NewGenerator) *Runner {
	rctx, rcan := context.WithCancel(ctx)
	return &Runner{
		requestrate:  requestrate,
		concurrency:  concurrency,
		warmup:       warmup,
		duration:     duration,
		newGenerator: newGenerator,
		ctx:          rctx,
		cancel:       rcan,
		stats:        s,
	}
}

func (r *Runner) Run() {
	r.workChan = make(chan interface{}, r.requestrate+2)

	var initDoneWg, runDoneWg sync.WaitGroup

	initDoneWg.Add(r.concurrency)
	runDoneWg.Add(r.concurrency)

	log.Debugf("Starting %d workers", r.concurrency)
	for i := 0; i < r.concurrency; i++ {
		lg := loadgen.NewLoadGenerator(i+1, r.requestrate, r.concurrency, r.newGenerator, r.workChan, r.ctx, r.stats)

		go func() {
			defer runDoneWg.Done()

			err := lg.Init()
			initDoneWg.Done()
			if err != nil {
				log.Errorf("Initialization failed: %v", err)
				return
			}

			// Wait for all goroutines to finish initialization
			initDoneWg.Wait()

			lg.Run()
		}()
	}

	// Wait for the initialization to be done
	initDoneWg.Wait()

	log.Infof("Starting ...")

	r.stats.ResetMetrics()

	// Ticker to generate work at constant throughput
	log.Debug("Starting work ticker")
	go r.ticker()

	// Start the warmup
	log.Debug("Starting warmup")
	go r.warmupTimer()

	// Timer to stop after specified duration
	log.Debug("Starting duration timer")
	go r.durationTimer()

	// Wait for all workers to quit
	log.Debug("Waiting for workers to finish")
	runDoneWg.Wait()

	// Print stats
	log.Debug("Printing statistics")
	fmt.Print(r.stats.Report())
}

func (r *Runner) Stop() {
	r.cancel()
}

func (r *Runner) ticker() {
	if r.requestrate == 0 {
		close(r.workChan)
		return
	}

	limiter := rate.NewLimiter(rate.Limit(r.requestrate), 2)

	cnt := 0
	for {
		if err := limiter.Wait(r.ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Errorf("Error waiting: %v", err)
			}
			return
		}

		select {
		case r.workChan <- nil:
		default:
			if cnt == 0 || cnt%100 == 0 {
				log.Warnf("Target host is likely slow: missed request rate (current=%v)", len(r.workChan))
			}
			cnt++
		}
	}
}

func (r *Runner) warmupTimer() {
	if r.warmup == 0 {
		return
	}

	// timer.Ticker doesn't have instant tick, so we will wait one more
	// extra second
	warmupTimer := time.NewTimer(r.warmup)

	select {
	case <-warmupTimer.C:
		warmupTimer.Stop()
		log.Infof("Warmup done (%v seconds)", r.warmup)
		r.stats.ResetMetrics()
		return
	case <-r.ctx.Done():
		warmupTimer.Stop()
		return
	}
}

func (r *Runner) durationTimer() {
	if r.duration == 0 {
		return
	}

	durationTimer := time.NewTimer(r.duration + r.warmup)

	select {
	case <-durationTimer.C:
		durationTimer.Stop()
		r.Stop()
	case <-r.ctx.Done():
		durationTimer.Stop()
		return
	}
}
