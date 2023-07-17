package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/freshworks/load-generator/internal/cql"
	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/spf13/cobra"
)

var cqlCmd = &cobra.Command{
	Use:   "cql <target>",
	Short: "cassandra load generator",
	Long: `cassandra load generator over cql native protocol.
`,
	Example: `
# Run against Cassandra cluster distributing load on all available nodes
lg cql --requestrate 1 cql --username foo --password 1234 --plaintext --query 'select * from mykeyspace.mytable where id=1234' localhost:9042

# Run against given node only, do not discovery peers from systems.peers table
lg cql --requestrate 1 cql --disable-peers-lookup --username foo --password 1234 --plaintext --query 'select * from mykeyspace.mytable where id=1234' localhost:9042
`,
	Args: cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := cql.NewOptions()
			o.Targets = args
			o.Query = cqlQuery
			o.Plaintext = cqlPlaintext
			o.AstraSecureBundle = cqlAstraSecureBundle
			o.Username = cqlUsername
			o.Password = cqlPassword
			o.DisablePeersLookup = cqlDisablePeersLookup
			o.Consistency = cqlConsistencyLevel
			o.ConnectTimeout = cqlConnectTimeout
			o.WriteTimeout = cqlWriteTimeout
			o.EnableCompression = cqlEnableCompression
			o.NumConnsPerHost = cqlNumConnsPerHost
			o.NumRetries = cqlNumRetries
			o.HostSelectionPolicy = cqlHostSelectionPolicy
			o.DCName = cqlDataCenterName
			o.WriteCoalesceWaitTime = cqlWriteCoalesceWaitTime
			o.Keyspace = cqlKeyspace
			o.TrackMetricsPerNode = cqlTrackMetricsPerNode

			return cql.NewGenerator(id, *o, ctx, requestrate, s)
		}

		if len(args) == 0 && cqlAstraSecureBundle == "" {
			return fmt.Errorf("target cassandra server was not given")
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var (
	cqlQuery                 string
	cqlPlaintext             bool
	cqlAstraSecureBundle     string
	cqlUsername              string
	cqlPassword              string
	cqlDisablePeersLookup    bool
	cqlConnectTimeout        time.Duration
	cqlWriteTimeout          time.Duration
	cqlConsistencyLevel      string
	cqlEnableCompression     bool
	cqlNumRetries            int
	cqlNumConnsPerHost       int
	cqlHostSelectionPolicy   string
	cqlDataCenterName        string
	cqlWriteCoalesceWaitTime time.Duration
	cqlKeyspace              string
	cqlTrackMetricsPerNode   bool
)

func init() {
	rootCmd.AddCommand(cqlCmd)
	cqlCmd.Flags().StringVar(&cqlQuery, "query", "SELECT uuid() FROM system.local", "Query")
	cqlCmd.Flags().BoolVar(&cqlPlaintext, "plaintext", false, "Use plaintext transport")
	cqlCmd.Flags().BoolVar(&cqlDisablePeersLookup, "disable-peers-lookup", false, "Do not lookup peers from systems table, just use provided hosts")
	cqlCmd.Flags().StringVar(&cqlAstraSecureBundle, "astra-secure-bundle", "", "Astra secure bundle zip file")
	cqlCmd.Flags().StringVar(&cqlUsername, "username", "", "username")
	cqlCmd.Flags().StringVar(&cqlPassword, "password", "", "password")
	cqlCmd.Flags().DurationVar(&cqlConnectTimeout, "connect-timeout", 5000*time.Second, "Connection timeout")
	cqlCmd.Flags().DurationVar(&cqlWriteTimeout, "write-timeout", 5000*time.Second, "Write timeout")
	cqlCmd.Flags().StringVar(&cqlConsistencyLevel, "consistency", "LOCAL_QUORUM", "Consistency level, possible values: ANY ONE TWO THREE QUORUM ALL LOCAL_QUORUM EACH_QUORUM LOCAL_ONE")
	cqlCmd.Flags().BoolVar(&cqlEnableCompression, "enable-compression", false, "Enable compression")
	cqlCmd.Flags().IntVar(&cqlNumConnsPerHost, "num-conns-per-host", 2, "How many connections to establish to each host")
	cqlCmd.Flags().IntVar(&cqlNumRetries, "num-retries", 0, "How many times to retry failed queries")
	cqlCmd.Flags().StringVar(&cqlHostSelectionPolicy, "host-selection-policy", "RoundRobin", "Host selection policy, possible values: RoundRobin DCAwareRoundRobin TokenAwareWithRoundRobinFallback TokenAwareWithDCAwareRoundRobinFallback")
	cqlCmd.Flags().StringVar(&cqlDataCenterName, "dc-name", "", "Datacenter name, needed based on host selection policy chosen")
	cqlCmd.Flags().DurationVar(&cqlWriteCoalesceWaitTime, "write-coalesce-wait-time", 0, "The time to wait for coalescing multiple frames before flushing")
	cqlCmd.Flags().StringVar(&cqlKeyspace, "keyspace", "", "Keyspace to operate on")
	cqlCmd.Flags().BoolVar(&cqlTrackMetricsPerNode, "track-metrics-per-node", false, "Track overall metrics and as well as per node")
}
