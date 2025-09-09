package cmd

import (
	"context"

	"github.com/freshworks/load-generator/internal/clickhouse"
	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var clickhouseCmd = &cobra.Command{
	Use:   "clickhouse [DSN]",
	Short: "ClickHouse load generator",
	Long: `ClickHouse load generator

Generates ClickHouse load using given query. It reports metrics by fingerprinting SQL query.

DSN format: clickhouse://username:password@host:port/database?param1=value1&param2=value2

Examples:
  clickhouse://127.0.0.1:9000/default
  clickhouse://user:pass@host:9000/db?dial_timeout=10s&compress=lz4
  clickhouse://user:pass@host:8123/db?protocol=http
`,
	Example: `
lg clickhouse --requestrate 1 'clickhouse://127.0.0.1:9000/default'
lg clickhouse --requestrate 10 --duration 10s --query 'SELECT count() FROM table' 'clickhouse://user:pass@host:9000/db'
`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			o := clickhouse.NewOptions()
			logrus.Infof("Using default DSN: %v", o.DSN)
			args = []string{o.DSN}
		}

		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := clickhouse.NewOptions()
			o.DSN = args[0]
			o.Query = clickhouseQuery
			return clickhouse.NewGenerator(id, *o, cmd.Context(), requestrate, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var (
	clickhouseQuery string
)

func init() {
	rootCmd.AddCommand(clickhouseCmd)
	clickhouseCmd.Flags().StringVar(&clickhouseQuery, "query", "SELECT 1", "Query to execute")
}
