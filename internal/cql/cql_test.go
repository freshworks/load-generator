package cql

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/gocql/gocql"
	"github.com/ory/dockertest/v3"
	mysqlquery "github.com/percona/go-mysql/query"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestCql(t *testing.T) {
	require := require.New(t)

	var sess *gocql.Session

	pool, resource, err := startCassandra()
	require.NoError(err)
	defer func() {
		if sess != nil {
			sess.Close()
		}
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	options := NewOptions()
	options.Username = "cassandra"
	options.Password = "cassandra"
	options.Plaintext = true
	options.Targets = []string{fmt.Sprintf("127.0.0.1:%s", resource.GetPort("9042/tcp"))}
	options.Query = "SELECT * from system.local"

	sts := stats.New("id", 1, 1, 0, false)
	sts.Start()
	defer sts.Stop()

	logger, _ := test.NewNullLogger()
	require.NotNil(logger)

	t.Run("Basic", func(t *testing.T) {
		//logrus.SetLevel(logrus.DebugLevel)
		g := NewGenerator(0, *options, context.Background(), 1, sts)
		require.NotNil(g)

		sess = g.Session

		sts.Reset()

		err := g.Init()
		require.Nil(err)

		err = g.InitDone()
		require.Nil(err)

		err = g.Tick()
		require.Nil(err)
		r := getStatResultFor(sts, options.sessionKey(), options.Query)
		require.NotNil(r)
		require.Equal(int64(1), r.Histogram.Count)

		err = g.Tick()
		require.Nil(err)
		r = getStatResultFor(sts, options.sessionKey(), options.Query)
		require.NotNil(r)
		require.Equal(int64(2), r.Histogram.Count)

		err = g.Finish()
		require.Nil(err)
	})
}

func startCassandra() (*dockertest.Pool, *dockertest.Resource, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, err
	}
	pool.MaxWait = 60 * time.Second

	err = pool.Client.Ping()
	if err != nil {
		return nil, nil, err
	}

	o := &dockertest.RunOptions{
		Repository: "scylladb/scylla",
		Tag:        "5.1",
		Cmd: []string{
			// Copied from https://github.com/scylladb/scylladb/blob/master/test/cql-pytest/run.py#L197
			"--developer-mode", "1",
			"--ring-delay-ms", "0",
			"--collectd", "0",
			"--smp", "1",
			"--max-networking-io-control-blocks", "100",
			"--unsafe-bypass-fsync", "1",
			"--kernel-page-cache", "1",
			"--flush-schema-tables-after-modification", "false",
			"--auto-snapshot", "0",
			"--skip-wait-for-gossip-to-settle", "0",
			"--logger-log-level", "compaction=warn",
			"--logger-log-level", "migration_manager=warn",
			"--range-request-timeout-in-ms", "300000",
			"--read-request-timeout-in-ms", "300000",
			"--counter-write-request-timeout-in-ms", "300000",
			"--cas-contention-timeout-in-ms", "300000",
			"--truncate-request-timeout-in-ms", "300000",
			"--write-request-timeout-in-ms", "300000",
			"--request-timeout-in-ms", "300000",
			"--experimental-features=udf",
			"--experimental-features=keyspace-storage-options",
			"--enable-user-defined-functions", "1",
			"--authenticator", "PasswordAuthenticator",
			"--authorizer", "CassandraAuthorizer",
			"--strict-allow-filtering", "true",
			"--permissions-update-interval-in-ms", "100",
			"--permissions-validity-in-ms", "100",
		},
	}

	resource, err := pool.RunWithOptions(o)
	if err != nil {
		return nil, nil, err
	}

	retURL := fmt.Sprintf("127.0.0.1:%s", resource.GetPort("9042/tcp"))
	port, _ := strconv.Atoi(resource.GetPort("9042/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		clusterConfig := gocql.NewCluster(retURL)
		clusterConfig.Authenticator = gocql.PasswordAuthenticator{
			Username: "cassandra",
			Password: "cassandra",
		}
		clusterConfig.ProtoVersion = 4
		clusterConfig.Port = port
		log.Printf("%v", clusterConfig.Port)

		session, err := clusterConfig.CreateSession()
		if err != nil {
			return fmt.Errorf("error creating session: %s", err)
		}
		defer session.Close()
		return nil
	}); err != nil {
		return nil, nil, err
	}

	return pool, resource, nil
}

func getStatResultFor(s *stats.Stats, key string, query string) *stats.Result {
	subkey := mysqlquery.Id(mysqlquery.Fingerprint(query))

	r := s.Export()

	for _, rr := range r.Results {
		if key == rr.Target && subkey == rr.SubTarget {
			return &rr
		}
	}

	return nil
}
