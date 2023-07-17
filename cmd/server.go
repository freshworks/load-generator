package cmd

import (
	"github.com/freshworks/load-generator/internal/server"
	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server <listenport>",
	Short: "Server mode",
	Long: `Runs in server mode.
In server mode, it just runs without generating any load, receives the metrics from clients, aggregates the metrics and publishes them.
It also exposes UI for viewing the latency graphs.
`,
	Example: `
`,
	Args: cobra.ExactArgs(1),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return server.Run(stat, args[0], cmd.Context(), importReport, exportReport)
	},
}

var importReport string

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.Flags().StringVar(&importReport, "import", "", "Report to import")
}
