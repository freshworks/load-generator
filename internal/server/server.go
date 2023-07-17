package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
)

var lg *LG

type LG struct {
	stats        *stats.Stats
	importCount  int
	mux          sync.Mutex
	exportReport string
	importReport string
	report       *stats.Report
}

func Run(s *stats.Stats, addr string, ctx context.Context, importReport, exportReport string) error {
	lg = &LG{stats: s, importReport: importReport, exportReport: exportReport}
	lg.reset()

	if importReport != "" {
		r, err := os.Open(importReport)
		if err != nil {
			return err
		}

		var report stats.Report
		if err = json.NewDecoder(r).Decode(&report); err != nil {
			return fmt.Errorf("error importing report (%s): %s", importReport, err)
		}
		lg.report = &report
	}

	rpc.Register(lg)
	rpc.HandleHTTP()

	http.HandleFunc("/", httpHandler)

	h := &http.Server{Addr: addr}
	go func() {
		logrus.Info("Serving on http://", addr)
		if err := h.ListenAndServe(); err != nil {
			logrus.Warn(err)
		}
	}()

	<-ctx.Done()

	return h.Shutdown(ctx)
}

func (l *LG) ImportReport(report *stats.Report, reply *int) error {
	l.mux.Lock()
	defer l.mux.Unlock()

	if l.report != nil {
		return fmt.Errorf("Server is running in display only mode, not accepting metrics import")
	}

	if l.importCount == 0 {
		l.reset()
	}

	l.importCount++

	logrus.Infof("Importing stats: %+v\n", report)
	l.stats.Import(report)

	l.printMetrics(os.Stdout)
	return l.writeReport()
}

func (l *LG) writeReport() error {
	if l.exportReport == "" {
		return nil
	}

	res := l.stats.Export()
	j, err := json.MarshalIndent(res, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(l.exportReport, j, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func (l *LG) printMetrics(w io.Writer) {
	fmt.Fprintf(w, "%v\n", l.stats.Report())
	return
}

func (l *LG) reset() {
	l.stats.Reset()
}
