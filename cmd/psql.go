package cmd

import (
	"context"

	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/psql"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	psqlQuery string
)

var psqlCmd = &cobra.Command{
	Use:   "psql <target>",
	Short: "Postgres load generator",
	Long: `Postgres load generator

Generates psql load using given query. It reports metrics by fingerprinting SQL query.
`,
	Example: `
	./lg psql --query "select 'Hello World!'" --requestrate 1 --duration 10s "postgresql://postgres@127.0.0.1:5432/"
	`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := psql.NewOptions()
			if len(args) == 0 {
				logrus.Warnf("connection string not specified, using default settings: %v", o.ConnectionString)
			} else if len(args) == 1 {
				o.ConnectionString = args[0]
			}
			o.Query = psqlQuery
			return psql.NewGenerator(id, *o, ctx, requestrate, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

func init() {
	rootCmd.AddCommand(psqlCmd)
	psqlCmd.Flags().StringVar(&psqlQuery, "query", "SELECT 1", "Query")
}
