package cmd

import (
	"context"

	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/mysql"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var mysqlCmd = &cobra.Command{
	Use:   "mysql username:password@tcp(host:port)/database?param1=value1&param2=value2",
	Short: "MySQL load generator",
	Long: `MySQL load generator

Generates MySQL load using given query. It reports metrics by fingerprinting SQL query.
`,
	Example: `
lg mysql --requestrate 1  'root@tcp(127.0.0.1:3306)/'
lg mysql --requestrate 10 --duration 10s --query 'SHOW DATABASES' 'myuser:mypassword@tcp(myserver.com:3306)/mydb'
`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			o := mysql.NewOptions()
			logrus.Infof("Using default DSN: %v", o.Target)
			args = []string{o.Target}
		}

		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := mysql.NewOptions()
			o.Target = args[0]
			o.Query = mysqlQuery
			return mysql.NewGenerator(id, *o, cmd.Context(), requestrate, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var (
	mysqlQuery string
)

func init() {
	rootCmd.AddCommand(mysqlCmd)
	mysqlCmd.Flags().StringVar(&mysqlQuery, "query", "SELECT 1", "Query")
}
