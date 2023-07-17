package cmd

import (
	"context"

	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/lua"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/spf13/cobra"
)

var scriptCmd = &cobra.Command{
	Use:                   "script </path/to/script.lua> [flags] -- [arguments to script]",
	DisableFlagsInUseLine: true,
	Short:                 "Script based load generator",
	Long: `Script based load generator

Generate load by using Lua script. All other load generating methods supported by lg is available for Lua script.
Payloads can be customized on the fly.
Load generation can be combined ("make one grpc call, then make another http call, together (and separately) report metrics" etc)
All the metrics are transparently collected and reported at the end.
`,
	Example: `
lg script /path/to/my/script.lua
lg script --requestrate 10 ./scripts/test.lua -- --foo bar
`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := lua.NewOptions()
			o.Script = args[0]
			o.Debug = debug

			n := cmd.ArgsLenAtDash()
			if n >= 0 {
				o.Args = args[n:]
			}

			return lua.NewGenerator(*o, id, requestrate, concurrency, ctx, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var scriptDebug bool

func init() {
	rootCmd.AddCommand(scriptCmd)
	scriptCmd.Flags().BoolVar(&scriptDebug, "debug", false, "Debug Lua script")
}
