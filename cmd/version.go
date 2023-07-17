package cmd

import (
	"fmt"

	rtdebug "runtime/debug"

	"github.com/spf13/cobra"
)

var Version string

var versionCmd = &cobra.Command{
	Use:           "version",
	Short:         "Prints the version",
	SilenceUsage:  true,
	SilenceErrors: true,
	Run: func(*cobra.Command, []string) {
		var revision string
		var dirty bool
		if info, ok := rtdebug.ReadBuildInfo(); ok {
			for _, setting := range info.Settings {
				switch setting.Key {
				case "vcs.revision":
					revision = setting.Value
				case "vcs.modified":
					dirty = setting.Value == "true"
				}
			}
		}

		fmt.Printf("Version: %s\n", Version)
		fmt.Printf("Revision: %s\n", revision)
		if dirty {
			fmt.Printf("DirtyBuild: true\n")
		}
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
