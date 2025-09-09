package clickhouse

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	mysqlquery "github.com/percona/go-mysql/query"
	"github.com/stretchr/testify/require"
)

var (
	// clickhouse versions to test on
	// corresponds to container image tags
	clickhouseTags = []string{
		"latest",
		"24.8",
		"24.3",
	}
)

func TestClickHouse(t *testing.T) {

	clickhouseTestOptions := map[ /*property*/ string] /*optional value*/ string{
		"Basic":            "",
		"WithAuth":         "",
		"InvalidQueryUsed": "",
	}

	for runKey, runValue := range clickhouseTestOptions {
		t.Run(fmt.Sprintf("ClickHouse/%v%v", runKey, runValue), func(t *testing.T) {

			var connectionString string
			var cleanupTestResources func()
			var err error
			query := "SELECT 1"
			require := require.New(t)

			if runKey == "WithAuth" {
				connectionString, cleanupTestResources, err = RunClickHouseDBWithAuth("latest")
			} else {
				connectionString, cleanupTestResources, err = RunClickHouseDB("latest")
			}

			if runKey == "InvalidQueryUsed" {
				query = "INVALID QUERY"
			}

			defer cleanupTestResources()
			if err != nil {
				log.Fatalf("Unable to get connection to ClickHouse Server: %v", err.Error())
			}
			genOptions := GeneratorOptions{
				DSN:   connectionString,
				Query: query,
			}

			sts := stats.New("id", 1, 1, 0, false)
			sts.Start()
			defer sts.Stop()

			//logrus.SetLevel(logrus.DebugLevel)
			g := NewGenerator(0, genOptions, context.Background(), 1, sts)
			require.NotNil(g)

			sts.Reset()

			err = g.Init()
			require.Nil(err)

			err = g.InitDone()
			require.Nil(err)

			err = g.Tick()
			if runKey == "InvalidQueryUsed" {
				// error queries should return an error and not be recorded
				require.Error(err)
				r := getStatResultFor(sts, connectionString, genOptions.Query)
				require.Nil(r)
				return
			}
			require.Nil(err)

			r := getStatResultFor(sts, connectionString, genOptions.Query)
			require.NotNil(r)
			require.Equal(int64(1), r.Histogram.Count)

			err = g.Tick()
			require.Nil(err)
			r = getStatResultFor(sts, connectionString, genOptions.Query)
			require.NotNil(r)
			require.Equal(int64(2), r.Histogram.Count)

			err = g.Finish()
			require.Nil(err)
		})
	}

}

// RunClickHouseDB creates a ClickHouse instance in Docker for testing
func RunClickHouseDB(tag string) (string, func(), error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return "", nil, err
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "clickhouse/clickhouse-server",
		Tag:        tag,
		Env: []string{
			"CLICKHOUSE_DB=default",
			"CLICKHOUSE_USER=default",
			"CLICKHOUSE_PASSWORD=",
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		return "", nil, err
	}

	dsn := fmt.Sprintf("clickhouse://127.0.0.1:%s/default", resource.GetPort("9000/tcp"))

	cleanup := func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s", err)
		}
	}

			// Wait for ClickHouse to be ready
			if err := pool.Retry(func() error {
				db := NewGenerator(0, GeneratorOptions{DSN: dsn}, context.Background(), 1, nil)
				defer db.Finish()
				return db.Init()
			}); err != nil {
				cleanup()
				return "", nil, err
			}

	return dsn, cleanup, nil
}

// RunClickHouseDBWithAuth creates a ClickHouse instance with authentication
func RunClickHouseDBWithAuth(tag string) (string, func(), error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return "", nil, err
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "clickhouse/clickhouse-server",
		Tag:        tag,
		Env: []string{
			"CLICKHOUSE_DB=default",
			"CLICKHOUSE_USER=testuser",
			"CLICKHOUSE_PASSWORD=testpass",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		return "", nil, err
	}

	dsn := fmt.Sprintf("clickhouse://testuser:testpass@127.0.0.1:%s/default", resource.GetPort("9000/tcp"))

	cleanup := func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s", err)
		}
	}

	// Wait for ClickHouse to be ready
	if err := pool.Retry(func() error {
		db := NewGenerator(0, GeneratorOptions{DSN: dsn}, context.Background(), 1, nil)
		defer db.Finish()
		return db.Init()
	}); err != nil {
		cleanup()
		return "", nil, err
	}

	return dsn, cleanup, nil
}

// Helper function to get stats result (similar to psql tests)
func getStatResultFor(sts *stats.Stats, key, query string) *stats.Result {
	report := sts.Export()
	// For ClickHouse, we need to check both the original query and the fingerprinted version
	for _, result := range report.Results {
		if result.Target == key && (result.SubTarget == query || result.SubTarget == mysqlquery.Id(mysqlquery.Fingerprint(query))) {
			return &result
		}
	}
	return nil
}
