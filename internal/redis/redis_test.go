package redis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedis(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	svr, err := miniredis.Run()
	require.NoError(err)
	defer svr.Close()

	options := NewOptions()
	options.Target = svr.Addr()
	options.Cmd = "get"
	options.Args = []string{"hello"}

	sts := stats.New("id", 1, 1, 0, false)
	sts.Start()
	defer sts.Stop()

	logger, _ := test.NewNullLogger()
	require.NotNil(logger)
	//log := logger.WithField("id", "1")

	t.Run("Basic", func(t *testing.T) {
		//logrus.SetLevel(logrus.DebugLevel)
		g := NewGenerator(0, *options, context.Background(), 1, sts)
		require.NotNil(g)
		sts.Reset()

		err := g.Init()
		assert.Nil(err)
		r := getStatResultFor(sts, options.Target, "ping")
		require.NotNil(r)
		assert.Equal(int64(1), r.Histogram.Count)

		err = g.InitDone()
		assert.Nil(err)

		err = g.Tick()
		assert.Nil(err)
		r = getStatResultFor(sts, options.Target, options.Cmd)
		require.NotNil(r)
		assert.Equal(int64(1), r.Histogram.Count)

		err = g.Tick()
		assert.Nil(err)
		r = getStatResultFor(sts, options.Target, options.Cmd)
		require.NotNil(r)
		assert.Equal(int64(2), r.Histogram.Count)

		err = g.Finish()
		assert.Nil(err)
	})

}

func getStatResultFor(s *stats.Stats, key string, subkey string) *stats.Result {
	r := s.Export()

	for _, rr := range r.Results {
		if key == rr.Target && subkey == rr.SubTarget {
			return &rr
		}
	}

	return nil
}
