package cmd

import (
	"context"

	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/redis"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/spf13/cobra"
)

var redisCmd = &cobra.Command{
	Use:   "redis <target>",
	Short: "Redis load generator",
	Long: `Redis load generator

Generates redis load using given command/arg.
`,
	Example: `
lg redis --requestrate 10 --cmd "GET" --arg "foo" 127.0.0.1:6379
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := redis.NewOptions()
			o.Target = args[0]
			o.Cmd = redisCommand
			o.Args = redisArgs
			o.Password = redisPassword
			o.Username = redisUsername
			o.Database = redisDatabase

			return redis.NewGenerator(id, *o, cmd.Context(), requestrate, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var (
	redisCommand  string
	redisArgs     []string
	redisPassword string
	redisUsername string
	redisDatabase int
)

func init() {
	rootCmd.AddCommand(redisCmd)
	redisCmd.Flags().StringVar(&redisCommand, "cmd", "", "Command")
	redisCmd.MarkFlagRequired("cmd")
	redisCmd.Flags().StringSliceVar(&redisArgs, "arg", []string{}, "Arguments to get command. Multiple values allowed")
	redisCmd.MarkFlagRequired("arg")
	redisCmd.Flags().StringVar(&redisPassword, "password", "", "Password")
	redisCmd.Flags().StringVar(&redisUsername, "username", "", "Username")
	redisCmd.Flags().IntVar(&redisDatabase, "database", 0, "Database")
}
