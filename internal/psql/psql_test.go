package psql

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/stretchr/testify/require"
)

var (
	// postgres server versions to test on
	// corresponds to container image tags
	tags = []string{
		"15",
		"14",
		"13",
	}
)

func TestPsql(t *testing.T) {

	psqlTestOptions := map[ /*property*/ string] /*optional value*/ string{
		"Basic":            "",
		"WithCerts":        "",
		"WrongCertUsed":    "",
		"InvalidQueryUsed": "",
	}

	for runKey, runValue := range psqlTestOptions {
		t.Run(fmt.Sprintf("PSQL/%v%v", runKey, runValue), func(t *testing.T) {

			var connectionString string
			var cleanupTestResources func()
			var err error
			query := "select 1"
			require := require.New(t)

			tempcert := &Cert{}
			tempcert.PopulateSelfSigned("test", "test.example.com")
			defer tempcert.Cleanup()
			if runKey == "WrongCertUsed" {
				connectionString, cleanupTestResources, err = RunPostgresDBWithWrongCerts("15", tempcert)
			} else {
				connectionString, cleanupTestResources, err = RunPostgresDB("15", tempcert)
			}

			if runKey == "InvalidQueryUsed" {
				query = "TYPO"
			}

			defer cleanupTestResources()
			if err != nil {
				log.Fatalf("Unable to get connection to PG Server: %v", err.Error())
			}
			genOptions := GeneratorOptions{
				ConnectionString: connectionString,
				Query:            query,
			}

			sts := stats.New("id", 1, 1, 0, false)
			sts.Start()
			defer sts.Stop()

			//logrus.SetLevel(logrus.DebugLevel)
			g := NewGenerator(0, genOptions, context.Background(), 1, sts)
			require.NotNil(g)

			sts.Reset()

			err = g.Init()
			if runKey == "WrongCertUsed" {
				require.ErrorContains(err, "certificate signed by unknown authority")
				return
			}
			require.Nil(err)

			err = g.InitDone()
			require.Nil(err)

			err = g.Tick()
			if runKey == "InvalidQueryUsed" {
				// error queries must not be recorded
				r := getStatResultFor(sts, "127.0.0.1", genOptions.Query)
				require.Nil(r)
				return
			}
			require.Nil(err)
			r := getStatResultFor(sts, "127.0.0.1", genOptions.Query)
			require.NotNil(r)
			require.Equal(int64(1), r.Histogram.Count)

			err = g.Tick()
			require.Nil(err)
			r = getStatResultFor(sts, "127.0.0.1", genOptions.Query)
			require.NotNil(r)
			require.Equal(int64(2), r.Histogram.Count)

			err = g.Finish()
			require.Nil(err)
		})
	}

}
