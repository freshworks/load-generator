package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var requestrate int
var concurrency int
var duration time.Duration
var warmup time.Duration
var debug bool
var verbose bool
var profile string
var exportReport string
var serverAddr string
var stat *stats.Stats
var id string

var rootCmd = &cobra.Command{
	Use:          "lg",
	SilenceUsage: true,
	Short:        "Load generator",
	Long: `Load generator
`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		id = uuid.New().String()

		if profile != "" {
			startProfile()
		}

		if verbose {
			logrus.SetLevel(logrus.DebugLevel)
		}

		if concurrency == 0 {
			concurrency = requestrate
		}

		if concurrency == 0 {
			concurrency = 1
		}

		stat = stats.New(id, requestrate, concurrency, duration, cmd.Name() == "server")
		stat.Start()

		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		defer stat.Stop()

		if profile != "" {
			finishProfile()
		}

		if exportReport != "" {
			err := writeReport()
			if err != nil {
				return err
			}
		}

		if serverAddr != "" {
			logrus.Infof("Publishing stats to %v\n", serverAddr)

			client, err := rpc.DialHTTP("tcp", serverAddr)
			if err != nil {
				return fmt.Errorf("cannot publish, error connecting to server: %v", err)
			}

			res := stat.Export()
			var reply int
			err = client.Call("LG.ImportReport", res, &reply)
			if err != nil {
				return fmt.Errorf("publish error: %v", err)
			}
		}

		return nil
	},
}

func Execute(ctx context.Context) {
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().IntVar(&requestrate, "requestrate", 1, "Request rate per second. =0 means no control on throughput")
	rootCmd.PersistentFlags().IntVar(&concurrency, "concurrency", 0, "Number of concurrent requests to make. Default is whatever specified in requestrate option")
	rootCmd.PersistentFlags().DurationVar(&duration, "duration", 0, "Number of seconds to run the test. 0 means forever")
	rootCmd.PersistentFlags().DurationVar(&warmup, "warmup", 5*time.Second, "warmup time")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Debug mode, useful to debug hung Lua scripts")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Set this to enable verbose logging")
	rootCmd.PersistentFlags().StringVar(&profile, "profile", "", "Generate cpu/memory profile file")
	rootCmd.PersistentFlags().StringVar(&exportReport, "export", "", "Export results in json format")
	rootCmd.PersistentFlags().StringVar(&serverAddr, "server", "", "Publish reports to remote lg server")
}

func initConfig() {
}

func startProfile() {
	cpuPofile := profile + ".cpu"
	logrus.Debugf("Starting CPU profile, writing to %v", cpuPofile)
	f, err := os.Create(cpuPofile)
	if err != nil {
		logrus.Fatalf("Could not create CPU profile file (%v): %v", cpuPofile, err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		logrus.Fatalf("Could not start CPU profile: %v", err)
	}
}

func finishProfile() {
	pprof.StopCPUProfile()

	memProfile := profile + ".mem"
	logrus.Debugf("Writing mem profile to %v", memProfile)
	runtime.GC()
	f, err := os.Create(memProfile)
	if err != nil {
		logrus.Fatalf("Could not create MEM profile file (%v): %v", memProfile, err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		logrus.Fatalf("Could not write Mem profile: %v", err)
	}
	f.Close()
}

func writeReport() error {
	res := stat.Export()
	j, err := json.MarshalIndent(res, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(exportReport, j, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
