package stats

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHistogram(t *testing.T) {
	require := require.New(t)

	hdrhist := hdrhistogram.New(1, maxHistogramValue, 3)

	min := int64(math.MaxInt64)
	max := int64(0)
	for i := 1; i <= 11; i++ {
		v := int64(i * 15)
		err := hdrhist.RecordValue(v)
		require.Nil(err)

		if v < min {
			min = v
		}

		if v > max {
			max = v
		}
	}

	var bars []hdrhistogram.Bar
	b := hdrhist.Distribution()
	for _, v := range b {
		if v.Count > 0 {
			bars = append(bars, v)
		}
	}

	require.NotZero(len(bars))
	require.LessOrEqual(hdrhist.Min(), min)
	require.GreaterOrEqual(hdrhist.Max(), max)

	buckets := getHistogramBuckets(hdrhist, scale)
	require.Equal(11, len(buckets))

	i := int64(1)
	for _, r := range buckets {
		require.Equal(int64(1), r.Count)
		require.Equal(float64(i*15)/1e3, r.Interval)
		i++
	}

	s := getResponseHistogram(buckets)
	require.NotEmpty(s)

	res := new(bytes.Buffer)
	for _, b := range buckets {
		res.WriteString(fmt.Sprintf("%10.3f [%10d]\t|%v\n", b.Interval, 1, strings.Repeat(barChar, int(40))))
	}
	require.Equal(res.String(), s)
}

func TestStats(t *testing.T) {
	id := uuid.New().String()
	s := New(id, 1, 1, 100*time.Second, false)
	s.Start()
	defer s.Stop()

	httpTrace := &TraceInfo{
		Type:   HttpTrace,
		Key:    "httptarget1",
		Subkey: "httpsubtarget1",
		Total:  200 * time.Millisecond,
		Error:  false,
		Status: 200,
	}
	s.RecordMetric(httpTrace)
	httpTrace2 := &TraceInfo{
		Type:   HttpTrace,
		Key:    "httptarget2",
		Subkey: "httpsubtarget2",
		Total:  200 * time.Millisecond,
		Error:  false,
		Status: 300,
	}
	s.RecordMetric(httpTrace2)
	httpTrace3 := &TraceInfo{
		Type:   HttpTrace,
		Key:    "httptarget3",
		Subkey: "httpsubtarget3",
		Total:  200 * time.Millisecond,
		Error:  false,
		Status: 400,
	}
	s.RecordMetric(httpTrace3)
	httpTrace4 := &TraceInfo{
		Type:   HttpTrace,
		Key:    "httptarget4",
		Subkey: "httpsubtarget4",
		Total:  200 * time.Millisecond,
		Error:  false,
		Status: 500,
	}
	s.RecordMetric(httpTrace4)
	httpTrace5 := &TraceInfo{
		Type:   HttpTrace,
		Key:    "httptarget5",
		Subkey: "httpsubtarget5",
		Total:  200 * time.Millisecond,
		Error:  true,
	}
	s.RecordMetric(httpTrace5)

	redisTrace := &TraceInfo{
		Type:   RedisTrace,
		Key:    "redistarget",
		Subkey: "redissubtarget",
		Total:  100 * time.Millisecond,
		Error:  true,
	}
	s.RecordMetric(redisTrace)

	sqlTrace := &TraceInfo{
		Type:   SqlTrace,
		Key:    "sqlstarget",
		Subkey: `SELECT * FROM SOMETHING WHERE foo="bar"`,
		Total:  100 * time.Millisecond,
		Error:  false,
	}
	s.RecordMetric(sqlTrace)

	rawTrace := &TraceInfo{
		Type: RawTrace,
		Key:  "rawtarget",
		//Subkey: `SELECT * FROM SOMETHING WHERE foo="bar"`,
		Total: 100,
		Error: false,
	}
	s.RecordMetric(rawTrace)

	report := s.Export()
	assert.NotNil(t, report)

	assert.Equal(t, report.Id, id)
	assert.Equal(t, report.Requestrate, 1)
	assert.Equal(t, report.Concurrency, 1)
	assert.Equal(t, report.Duration, fmt.Sprint(100*time.Second))

	assert.Equal(t, 8, len(report.Results))

	get := func(target, subtarget string) *Result {
		for _, r := range report.Results {
			if r.Target == target && r.SubTarget == subtarget {
				return &r
			}
		}

		return nil
	}
	for _, tr := range []*TraceInfo{redisTrace, httpTrace, httpTrace2, httpTrace3, httpTrace4, httpTrace5, sqlTrace, rawTrace} {
		r := get(tr.Key, tr.Subkey)
		require.NotNilf(t, r, "%+v", tr)

		assert.Equal(t, string(tr.Type), r.Type)
		assert.Equal(t, tr.Key, r.Target)
		assert.Equal(t, tr.Subkey, r.SubTarget)
		switch tr.Type {
		case HttpTrace:
			if tr.Status >= 500 {
				require.NotNil(t, r.Status5xx)
				assert.Equal(t, 1, *r.Status5xx)
			} else if tr.Status >= 400 {
				require.NotNil(t, r.Status4xx)
				assert.Equal(t, 1, *r.Status4xx)
			} else if tr.Status >= 300 {
				require.NotNil(t, r.Status3xx)
				assert.Equal(t, 1, *r.Status3xx)
			} else if tr.Status >= 200 {
				require.NotNil(t, r.Status2xx)
				assert.Equal(t, 1, *r.Status2xx)
			}
		case SqlTrace:
			assert.Equal(t, "C6CD2BA55C3905A8", r.SubTarget)
			assert.Contains(t, report.DigestToQuery, "C6CD2BA55C3905A8")
			dd := report.DigestToQuery["C6CD2BA55C3905A8"]
			assert.Equalf(t, "select * from something where foo=?", dd, "%+v", report.DigestToQuery)
		}

		if tr.Error {
			require.NotNil(t, r.Errors)
			assert.Equalf(t, 1, *r.Errors, "%+v", tr)
		}

		if tr.Type == RawTrace {
			assert.Equal(t, float64(100), r.Histogram.Max)
		} else {
			assert.InEpsilon(t, r.Histogram.Max, float64(tr.Total/1e6), 0.1)
		}
	}

	// s.Reset()
	// report := s.Export()
}

func TestBarsToBuckets(t *testing.T) {
	require := require.New(t)

	bars := []hdrhistogram.Bar{
		{From: 5, To: 15, Count: 10},
		{From: 8, To: 20, Count: 5},
		{From: 21, To: 23, Count: 5},
		{From: 50, To: 100, Count: 5},
	}

	buckets := hdrHistBarsToBuckets(bars, 5, 100, 1)
	require.Equalf(3, len(buckets), "%+v", buckets)
	var count int64
	for _, b := range buckets {
		count += b.Count
	}
	require.Equal(int64(25), count)
}
