package stats

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/olekukonko/tablewriter"
	mysqlquery "github.com/percona/go-mysql/query"
	"github.com/sirupsen/logrus"
)

var (
	// 5 mins (in usecs)
	maxHistogramValue = int64(5 * 60 * 1000000)
	barChar           = "■"
	log               *logrus.Entry
	scale             = 1000.0
)

type Key string
type KeyMap map[Key]SubkeyMap

type Subkey string
type SubkeyMap map[Subkey]*Metrics

type MetricsMap map[TraceType]map[Key]map[Subkey]*Metrics

type Stats struct {
	id          string
	requestrate int
	concurrency int
	duration    time.Duration
	startTime   time.Time
	endTime     time.Time
	importCount int
	// This is where all the per request info is stored
	metrics       MetricsMap
	digestToQuery map[string]string
	statsChan     chan *TraceInfo
	statsWg       sync.WaitGroup
	statsCmd      chan statsCmd
	server        bool
}

type statsCmd struct {
	cmd  int
	arg  interface{}
	done chan interface{}
}

const (
	statsCmdReset        = iota // Reset evertything
	statsCmdResetMetrics        // Reset only metrics
	statsCmdPrint               // Print metrics
	statsCmdQuit                // Quit
	statsCmdExport              // Export metrics report
	statsCmdImport              // Import metrics report
)

type TraceType string

const (
	HttpTrace   TraceType = "http"
	GrpcTrace   TraceType = "grpc"
	RedisTrace  TraceType = "redis"
	SqlTrace    TraceType = "sql"
	PGTrace     TraceType = "psql"
	CqlTrace    TraceType = "cql"
	SmtpTrace   TraceType = "smtp"
	MongoTrace  TraceType = "mongo"
	CustomTrace TraceType = "custom"
	RawTrace    TraceType = "raw"
)

type TraceInfo struct {
	Type             TraceType
	Key              string
	Subkey           string
	Total            time.Duration
	Status           int
	Error            bool
	DeadlineExceeded bool
}

type Metrics struct {
	Type      TraceType
	latency   *hdrhistogram.Histogram
	Status5xx int
	Status4xx int
	Status3xx int
	Status2xx int
	Errors    int
	Errors2   int
	// For RPS calculation
	rps            *hdrhistogram.Histogram
	lastReftime    time.Time
	lastTotalCount int64
}

type Report struct {
	Id            string
	Requestrate   int
	Concurrency   int
	Duration      string
	StartTime     time.Time
	EndTime       time.Time
	NumWorkers    *int `json:",omitempty"`
	Results       []Result
	DigestToQuery map[string]string `json:",omitempty"`
}

type Result struct {
	Type            string
	Target          string
	SubTarget       string
	AvgRPS          float64
	Histogram       HistogramData
	Status5xx       *int                   `json:",omitempty"`
	Status4xx       *int                   `json:",omitempty"`
	Status3xx       *int                   `json:",omitempty"`
	Status2xx       *int                   `json:",omitempty"`
	Errors          *int                   `json:",omitempty"`
	Errors2         *int                   `json:",omitempty"`
	LatencySnapshot *hdrhistogram.Snapshot `json:"-"`
}

type HistogramData struct {
	Count       int64
	Min         float64
	Max         float64
	Sum         float64
	Avg         float64
	StdDev      float64
	Data        []Bucket
	Percentiles []Percentile
}

type Bucket struct {
	Interval float64
	Count    int64
	Percent  float64
}

type Percentile struct {
	Percentile float64
	Value      float64
}

func init() {
	log = logrus.WithFields(logrus.Fields{"Id": 0})
}

func New(id string, requestrate int, concurrency int, duration time.Duration, server bool) *Stats {
	r := requestrate * 100
	if r <= 0 {
		r = 10000
	}

	// This is where all the per request info is stored
	return &Stats{
		id:            id,
		requestrate:   requestrate,
		concurrency:   concurrency,
		duration:      duration,
		metrics:       newMetricsMap(),
		digestToQuery: make(map[string]string),
		statsChan:     make(chan *TraceInfo, r),
		statsCmd:      make(chan statsCmd),
		server:        server,
	}
}

func newMetricsMap() MetricsMap {
	return MetricsMap{}
}

func newKeyMap() map[Key]map[Subkey]*Metrics {
	return map[Key]map[Subkey]*Metrics{}
}

func newSubkeyMap() map[Subkey]*Metrics {
	return map[Subkey]*Metrics{}
}

func newMetrics() *Metrics {
	return &Metrics{
		latency: hdrhistogram.New(1, maxHistogramValue, 3),
		rps:     hdrhistogram.New(1, int64(10000000), 3),
	}
}

func (s *Stats) Start() {
	s.startTime = time.Now()
	s.statsWg.Add(1)
	go func() {
		defer s.statsWg.Done()
		s.statsCollector()
	}()
}

func (s *Stats) Stop() {
	done := make(chan interface{})
	s.statsCmd <- statsCmd{statsCmdQuit, nil, done}
	<-done
	s.statsWg.Wait()
}

func (s *Stats) Reset() {
	done := make(chan interface{})
	s.statsCmd <- statsCmd{statsCmdReset, nil, done}
	<-done
}

// Only reset metrics
func (s *Stats) ResetMetrics() {
	done := make(chan interface{})
	s.statsCmd <- statsCmd{statsCmdResetMetrics, nil, done}
	<-done
}

func (s *Stats) Report() string {
	done := make(chan interface{})
	s.statsCmd <- statsCmd{statsCmdPrint, nil, done}
	return (<-done).(string)
}

func (s *Stats) Export() *Report {
	done := make(chan interface{})
	s.statsCmd <- statsCmd{statsCmdExport, nil, done}
	return (<-done).(*Report)
}

func (s *Stats) Import(report *Report) {
	done := make(chan interface{})
	s.statsCmd <- statsCmd{statsCmdImport, report, done}
	<-done
}

func (mm MetricsMap) update(t *TraceInfo) {
	m := mm.getMetrics(t.Type, Key(t.Key), Subkey(t.Subkey))

	switch t.Type {
	case HttpTrace:
		switch {
		case t.Status >= 500:
			m.Status5xx++
		case t.Status >= 400:
			m.Status4xx++
		case t.Status >= 300:
			m.Status3xx++
		case t.Status >= 200:
			m.Status2xx++
		}
	case GrpcTrace:
		if t.DeadlineExceeded {
			m.Errors2++
		}
	}

	if t.Error {
		m.Errors++
	}

	if t.Total != 0 {
		if t.Type != RawTrace {
			m.Add(int64(t.Total / time.Microsecond))
		} else {
			m.Add(int64(t.Total))
		}
	}
}

func (mm MetricsMap) updateRPS() {
	for _, m := range mm {
		for _, v := range m {
			for _, v2 := range v {
				v2.updateRPS()
			}
		}
	}
}

func (mm MetricsMap) export() []Result {
	results := []Result{}
	for _, m := range mm {
		for key, v := range m {
			for subkey, m := range v {
				actualScale := scale
				if m.Type == RawTrace {
					actualScale = 1
				}

				r := Result{
					Type:      string(m.Type),
					Target:    string(key),
					SubTarget: string(subkey),
					AvgRPS:    m.rps.Mean(),
					Histogram: HistogramData{
						Min:    float64(m.latency.Min()) / actualScale,
						Max:    float64(m.latency.Max()) / actualScale,
						Avg:    m.latency.Mean() / actualScale,
						StdDev: m.latency.StdDev() / actualScale,
						//Sum:    m.latency.Sum(),
						Count: m.latency.TotalCount(),
						Data:  getHistogramBuckets(m.latency, actualScale),
						Percentiles: []Percentile{
							Percentile{50.0, float64(m.latency.ValueAtQuantile(50)) / actualScale},
							Percentile{75.0, float64(m.latency.ValueAtQuantile(75)) / actualScale},
							Percentile{90.0, float64(m.latency.ValueAtQuantile(90)) / actualScale},
							Percentile{95.0, float64(m.latency.ValueAtQuantile(95)) / actualScale},
							Percentile{99.0, float64(m.latency.ValueAtQuantile(99)) / actualScale},
							Percentile{99.99, float64(m.latency.ValueAtQuantile(99.99)) / actualScale},
						},
					},
					Errors:          intPtr(m.Errors),
					Errors2:         intPtr(m.Errors2),
					LatencySnapshot: m.latency.Export(),
				}

				if m.Type == HttpTrace {
					r.Status2xx = intPtr(m.Status2xx)
					r.Status3xx = intPtr(m.Status3xx)
					r.Status4xx = intPtr(m.Status4xx)
					r.Status5xx = intPtr(m.Status5xx)
				}

				results = append(results, r)
			}
		}
	}

	sort.SliceStable(results[:], func(i, j int) bool {
		if strings.Compare(results[i].Target, results[j].Target) < 0 {
			return true
		}
		if strings.Compare(results[i].Target, results[j].Target) > 0 {
			return false
		}

		if strings.Compare(results[i].SubTarget, results[j].SubTarget) < 0 {
			return true
		}
		if strings.Compare(results[i].SubTarget, results[j].SubTarget) > 0 {
			return false
		}

		return false
	})

	return results
}

func (mm MetricsMap) importReport(report *Report) {
	for _, r := range report.Results {
		t := TraceType(r.Type)
		key := Key(r.Target)
		subkey := Subkey(r.SubTarget)
		m := mm.getMetrics(t, key, subkey)

		m.Type = t

		if r.Status2xx != nil {
			m.Status2xx += *r.Status2xx
		}
		if r.Status3xx != nil {
			m.Status3xx += *r.Status3xx
		}
		if r.Status4xx != nil {
			m.Status4xx += *r.Status4xx
		}
		if r.Status5xx != nil {
			m.Status5xx += *r.Status5xx
		}
		if r.Errors != nil {
			m.Errors += *r.Errors
		}

		if r.Errors2 != nil {
			m.Errors2 += *r.Errors2
		}

		d := m.latency.Merge(hdrhistogram.Import(r.LatencySnapshot))
		if d != 0 {
			logrus.Warnf("Dropped latency metrics: %v", d)
		}

		// Don't merge, add the rps. This assumes all clients run in
		// parallel and send the results
		//
		// TODO: Should this be exposed as an option?
		current := m.rps.Mean()
		m.rps.Reset()
		err := m.rps.RecordValue(int64(math.Round(current + r.AvgRPS)))
		if err != nil {
			logrus.Warnf("Dropped rps metrics: %v", d)
		}
	}
}

func (mm MetricsMap) print() string {
	var out strings.Builder

	for typ, v1 := range mm {
		// Sort by host name
		type kv struct {
			key  Key
			resp map[Subkey]*Metrics
		}

		stats := make([]kv, 0, len(v1))
		for key, v := range v1 {
			stats = append(stats, kv{key, v})
		}
		sort.SliceStable(stats[:], func(i, j int) bool {
			return stats[i].key < stats[j].key
		})

		for _, v2 := range stats {
			// Sort by request count
			type kv struct {
				subkey Subkey
				resp   *Metrics
			}

			resps := make([]kv, 0, len(v2.resp))
			for subkey, r := range v2.resp {
				resps = append(resps, kv{subkey, r})
			}
			sort.SliceStable(resps[:], func(i, j int) bool {
				return resps[i].resp.latency.TotalCount() < resps[j].resp.latency.TotalCount()
			})

			var name string
			var subKeyDisplayName string
			var colsToPrint []string
			var colsNamesToDisplay []string

			actualScale := scale

			switch typ {
			case HttpTrace:
				name = "HTTP Metrics"
				subKeyDisplayName = "Url"
				colsToPrint = []string{"2xx", "3xx", "4xx", "5xx"}
				colsNamesToDisplay = []string{"2xx", "3xx", "4xx", "5xx"}

			case GrpcTrace:
				name = "GRPC Metrics"
				subKeyDisplayName = "Method"
				colsToPrint = []string{"errors2"}
				colsNamesToDisplay = []string{"deadline"}

			case SqlTrace:
				name = "MySQL Metrics"
				subKeyDisplayName = "Query"

			case PGTrace:
				name = "PostgresQL Metrics"
				subKeyDisplayName = "Query"

			case CqlTrace:
				name = "Cassandra Metrics"
				subKeyDisplayName = "Query"

			case RedisTrace:
				name = "Redis Metrics"
				subKeyDisplayName = "Command"

			case CustomTrace:
				name = "Custom Metrics"
				subKeyDisplayName = "Key"

			case SmtpTrace:
				name = "SMTP Metrics"
				subKeyDisplayName = "Key"

			case MongoTrace:
				name = "MongoDB Metrics"
				subKeyDisplayName = "Operation"

			case RawTrace:
				name = "Raw Metrics"
				subKeyDisplayName = "Key"
				actualScale = 1
			}

			fmt.Fprintf(&out, "\n%v:\n", name)
			fmt.Fprintf(&out, "\n%s:\n", v2.key)

			hdrs := []string{subKeyDisplayName, "Avg", "StdDev", "Min",
				"Max", "p50", "p95", "p99", "p99.99", "Total"}
			if typ != RawTrace {
				hdrs = append(hdrs, []string{"AvgRPS", "Errors"}...)
			}
			hdrs = append(hdrs, colsNamesToDisplay...)

			table := tablewriter.NewTable(&out)
			hdrInterfaces := make([]any, len(hdrs))
			for i, h := range hdrs {
				hdrInterfaces[i] = h
			}
			table.Header(hdrInterfaces...)

			for _, u := range resps {
				records := []string{
					string(u.subkey),
					strconv.FormatFloat(u.resp.latency.Mean()/actualScale, 'f', 2, 64),
					strconv.FormatFloat(u.resp.latency.StdDev()/actualScale, 'f', 2, 64),
					strconv.FormatFloat(float64(u.resp.latency.Min())/actualScale, 'f', 2, 64),
					strconv.FormatFloat(float64(u.resp.latency.Max())/actualScale, 'f', 2, 64),
					strconv.FormatFloat(float64(u.resp.latency.ValueAtQuantile(50))/actualScale, 'f', 2, 64),
					strconv.FormatFloat(float64(u.resp.latency.ValueAtQuantile(95))/actualScale, 'f', 2, 64),
					strconv.FormatFloat(float64(u.resp.latency.ValueAtQuantile(99))/actualScale, 'f', 2, 64),
					strconv.FormatFloat(float64(u.resp.latency.ValueAtQuantile(99.99))/actualScale, 'f', 2, 64),
					strconv.FormatInt(u.resp.latency.TotalCount(), 10),
				}
				if typ != RawTrace {
					records = append(records, []string{
						strconv.FormatFloat(u.resp.rps.Mean(), 'f', 2, 64),
						strconv.FormatInt(int64(u.resp.Errors), 10)}...)
				}

				for _, c := range colsToPrint {
					switch c {
					case "2xx":
						records = append(records, strconv.FormatInt(int64(u.resp.Status2xx), 10))
					case "3xx":
						records = append(records, strconv.FormatInt(int64(u.resp.Status3xx), 10))
					case "4xx":
						records = append(records, strconv.FormatInt(int64(u.resp.Status4xx), 10))
					case "5xx":
						records = append(records, strconv.FormatInt(int64(u.resp.Status5xx), 10))
					case "errors2":
						records = append(records, strconv.FormatInt(int64(u.resp.Errors2), 10))
					default:
						logrus.Errorf("Unknown column to print: %v", c)
					}
				}

				table.Append(records)
			}
			table.Render()

			desc := ""
			if typ != RawTrace {
				desc = "Response time histogram (ms):"
			}
			for _, u := range resps {
				fmt.Fprintf(&out, "%s", printHistogram(string(u.subkey), desc, u.resp.latency, actualScale))
			}
		}
	}

	return out.String()
}

func (m *Metrics) Count() int {
	return int(m.latency.TotalCount())
}

func (m *Metrics) Add(rtime int64) {
	// Record response time
	err := m.latency.RecordValue(rtime)
	if err != nil {
		log.Warnf("Failed to add value to histogram: %s", err.Error())
	}

	//Log.Debugf("TotalRequests = %d", this.hdrhist.TotalCount())
}

func (m *Metrics) updateRPS() {
	n := time.Now()
	if m.lastReftime.IsZero() {
		m.lastReftime = n
		m.lastTotalCount = m.latency.TotalCount()
		return
	}

	totalCount := m.latency.TotalCount()
	v := totalCount - m.lastTotalCount

	d := float64(n.Sub(m.lastReftime)) / 1e6
	r := int64(math.Round(float64(v) / d * 1e3))
	if r != 0 {
		m.rps.RecordValue(r)
	}

	m.lastReftime = n
	m.lastTotalCount = totalCount
}

func (s *Stats) statsCollector() {
	t := time.NewTicker(1 * time.Second)
	if s.server {
		t.Stop()
	}

	for {
		select {
		case m := <-s.statsChan:
			s.handleMetric(m)
		case <-t.C:
			s.flush()
			s.statsRPSUpdate()

		case c := <-s.statsCmd:
			switch c.cmd {
			case statsCmdPrint:
				s.flush()
				c.done <- s.print()
				close(c.done)
			case statsCmdExport:
				s.flush()
				c.done <- s.export()
				close(c.done)
			case statsCmdImport:
				s.importReport(c.arg.(*Report))
				close(c.done)
			case statsCmdReset:
				s.reset()
				close(c.done)
			case statsCmdResetMetrics:
				s.resetMetrics()
				close(c.done)
			case statsCmdQuit:
				close(c.done)
				return
			}
		}
	}
}

func (s *Stats) reset() {
	s.resetMetrics()
	s.requestrate = 0
	s.concurrency = 0
	s.duration = 0
	s.digestToQuery = make(map[string]string)
}

func (s *Stats) resetMetrics() {
	s.metrics = newMetricsMap()
	s.startTime = time.Now()
	s.endTime = time.Now()
	s.importCount = 0
	s.digestToQuery = make(map[string]string)
}

func (s *Stats) handleMetric(t *TraceInfo) {
	if t.Type == SqlTrace || t.Type == CqlTrace || t.Type == PGTrace {
		q := mysqlquery.Fingerprint(t.Subkey)
		d := mysqlquery.Id(q)
		t.Subkey = d
		s.digestToQuery[d] = q
	}

	s.metrics.update(t)
}

func (s *Stats) RecordMetric(t *TraceInfo) {
	s.statsChan <- t
}

func (s *Stats) export() *Report {
	dq := map[string]string{}
	for k, v := range s.digestToQuery {
		dq[k] = v
	}

	if s.endTime.IsZero() {
		s.endTime = time.Now()
	}

	var w *int
	if s.importCount > 0 {
		w = intPtr(s.importCount)
	}

	return &Report{
		Id:            s.id,
		Requestrate:   s.requestrate,
		Concurrency:   s.concurrency,
		Duration:      fmt.Sprint(s.duration),
		StartTime:     s.startTime,
		EndTime:       s.endTime,
		Results:       s.metrics.export(),
		DigestToQuery: dq,
		NumWorkers:    w,
	}
}

func (s *Stats) importReport(report *Report) {

	s.requestrate += report.Requestrate

	s.concurrency += report.Concurrency

	d, _ := time.ParseDuration(report.Duration)
	s.duration += d // ??

	if s.startTime.After(report.StartTime) {
		s.startTime = report.StartTime
	}

	if s.endTime.Before(report.EndTime) {
		s.endTime = report.EndTime
	}

	s.importCount++

	s.metrics.importReport(report)

	for k, m := range report.DigestToQuery {
		s.digestToQuery[k] = m
	}
}

func (s *Stats) flush() {
	for {
		select {
		case m := <-s.statsChan:
			s.handleMetric(m)
		default:
			return
		}
	}
}

func (s *Stats) statsRPSUpdate() {
	s.metrics.updateRPS()
}

func (s *Stats) print() string {
	var out strings.Builder

	fmt.Fprintf(&out, "\n")
	if s.importCount > 0 {
		fmt.Fprintf(&out, "\nMerics collected from %v remote workers\n", s.importCount)
	}
	fmt.Fprintf(&out, "%v", s.metrics.print())
	if len(s.digestToQuery) > 0 {
		fmt.Fprintf(&out, "Digest to query mapping:\n")
		for k, v := range s.digestToQuery {
			fmt.Fprintf(&out, "  %s : %s\n", k, v)
		}
	}
	fmt.Fprintln(&out, "")

	return out.String()
}

func printHistogram(title string, description string, hdrhist *hdrhistogram.Histogram, scale float64) string {
	var o strings.Builder
	buckets := getHistogramBuckets(hdrhist, scale)
	if len(buckets) == 0 {
		log.Debug("No histogram buckets")
		return o.String()
	}

	if description != "" {
		fmt.Fprintf(&o, "\n%s\n", description)
	}
	fmt.Fprintf(&o, "\n%v:\n", title)
	fmt.Fprint(&o, getResponseHistogram(buckets))

	return o.String()
}

func getResponseHistogram(buckets []Bucket) string {
	var maxCount int64
	for _, b := range buckets {
		if b.Count > maxCount {
			maxCount = b.Count
		}
	}

	res := new(bytes.Buffer)
	for i := 0; i < len(buckets); i++ {
		var barLen int64
		if maxCount > 0 {
			barLen = (buckets[i].Count*40 + maxCount/2) / maxCount
		}
		res.WriteString(fmt.Sprintf("%10.3f [%10d]\t|%v\n", buckets[i].Interval, buckets[i].Count, strings.Repeat(barChar, int(barLen))))
	}

	return res.String()
}

func hdrHistBarsToBuckets(bars []hdrhistogram.Bar, min, max int64, scale float64) []Bucket {
	bc := int64(10)
	buckets := make([]int64, bc+1)
	counts := make([]int64, bc+1)
	bs := (max - min) / (bc)
	for i := int64(0); i < bc; i++ {
		buckets[i] = min + bs*(i)
	}

	buckets[bc] = max
	counts[bc] = bars[len(bars)-1].Count

	bi := 0
	for i := 0; i < len(bars)-1; {
		if bars[i].From <= buckets[bi] && bars[i].To <= buckets[bi] {
			// Entire bar is in this bucket
			counts[bi] += bars[i].Count
			i++
		} else if bars[i].From <= buckets[bi] && bars[i].To > buckets[bi] {
			// Bar overlaps this bucket
			// Take a ratio of the count based on the overlap
			rng := bars[i].To - bars[i].From
			rng_A := buckets[bi] - bars[i].From
			rng_B := bars[i].To - buckets[bi]
			counts[bi] += int64(math.Round(float64(bars[i].Count) * (float64(rng_A) / float64(rng))))
			if bi < len(buckets)-1 {
				bi++
			}
			counts[bi] += int64(math.Floor(float64(bars[i].Count) * (float64(rng_B) / float64(rng))))
			i++
		} else if bi < len(buckets)-1 {
			// Bar is after this bucket
			bi++
		}
	}

	var total int64
	for i := 0; i < len(buckets); i++ {
		total += counts[i]
	}

	res := []Bucket{}
	for i := 0; i < len(buckets); i++ {
		if counts[i] > 0 {
			res = append(res,
				Bucket{
					Interval: float64(buckets[i]) / scale,
					Count:    counts[i],
					Percent:  100.0 * float64(counts[i]) / float64(total),
				})
		}
	}

	return res
}

func getHistogramBuckets(hdrhist *hdrhistogram.Histogram, scale float64) []Bucket {
	var bars []hdrhistogram.Bar
	b := hdrhist.Distribution()
	for _, v := range b {
		if v.Count > 0 {
			bars = append(bars, v)
		}
	}

	if len(bars) == 0 {
		return []Bucket{}
	}

	return hdrHistBarsToBuckets(bars, hdrhist.Min(), hdrhist.Max(), scale)
}

func (mm MetricsMap) getMetrics(t TraceType, k Key, sk Subkey) *Metrics {
	mk, ok := mm[t]
	if !ok {
		mk = newKeyMap()
		mm[t] = mk
	}

	msk, ok := mk[k]
	if !ok {
		msk = newSubkeyMap()
		mk[k] = msk
	}

	m, ok := msk[sk]
	if !ok {
		m = newMetrics()
		m.Type = t
		msk[sk] = m
	}

	return m
}

func intPtr(i int) *int {
	return &i
}
