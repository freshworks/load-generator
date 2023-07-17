package cmd

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/freshworks/load-generator/internal/http"
	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/spf13/cobra"
)

var httpCmd = &cobra.Command{
	Use:   "http <target>",
	Short: "HTTP load generator",
	Long:  `HTTP load generator`,
	Example: `
lg http https://example.com.service/some/path
lg http --requestrate 10 http://example.com/some/path
`,
	Args: cobra.ExactArgs(1),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		u, err := url.ParseRequestURI(args[0])
		if err != nil {
			return err
		}

		if !(u.Scheme == "http" || u.Scheme == "https") {
			return fmt.Errorf("invalid url scheme: %s (%s)", u.Scheme, args[0])
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		u, _ := url.ParseRequestURI(args[0])
		if u.Path == "" {
			u.Path = "/"
		}

		amp := map[string]map[*regexp.Regexp]string{}
		for _, s := range httpAggregatePath {
			aggregateRegex := strings.Split(s, "|")

			if len(aggregateRegex) > 3 {
				return fmt.Errorf("error: found extra args for the aggregate regex pattern")
			}

			if len(aggregateRegex) < 2 {
				return fmt.Errorf("error: both pattern and replacement pattern should be given")
			}

			var aggregateMethod string
			if len(aggregateRegex) == 2 {
				// If no method is provided, use "any"
				aggregateMethod = "any"
			}

			if len(aggregateRegex) == 3 {
				aggregateMethod = aggregateRegex[2]
			}

			if _, ok := amp[aggregateMethod]; !ok {
				amp[aggregateMethod] = make(map[*regexp.Regexp]string)

			}
			amp[aggregateMethod][regexp.MustCompile(aggregateRegex[0])] = aggregateRegex[1]
		}

		hdr := map[string]string{}
		for _, s := range httpHeaders {
			n := strings.Index(s, ":")
			if n > 0 && n+1 < len(s) {
				hdr[strings.TrimSpace(s[:n])] = strings.TrimSpace(s[n+1:])
			}
		}

		proxyHdr := map[string]string{}
		for _, s := range httpProxyHeaders {
			n := strings.Index(s, ":")
			if n > 0 && n+1 < len(s) {
				proxyHdr[strings.TrimSpace(s[:n])] = strings.TrimSpace(s[n+1:])
			}
		}

		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := http.NewOptions()
			o.DiscardResponse = true
			o.Method = httpMethod
			o.Data = httpData
			o.KeepAlive = !httpNoKeepalive
			o.Insecure = httpInsecure
			o.AggregateMethodPath = amp
			o.Headers = hdr
			o.ProxyHeaders = proxyHdr
			o.TlsServerName = tlsServerName
			o.RootCAs = rootCAs

			o.Url = *u

			return http.NewGenerator(id, *o, cmd.Context(), requestrate, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var httpAggregatePath []string
var httpData string
var httpHeaders []string
var httpProxyHeaders []string
var httpInsecure bool
var rootCAs []string
var tlsServerName string
var httpMethod string
var httpNoKeepalive bool

func init() {
	rootCmd.AddCommand(httpCmd)

	httpCmd.Flags().StringVar(&httpMethod, "method", "GET", "HTTP method")
	httpCmd.Flags().StringSliceVar(&httpAggregatePath, "aggregate-path", []string{}, `Path to aggregate for stats. Ex: --aggregate-path "/api/_/tickets/[0-9]+$|/api/_/ticketupdate" --aggregate-path "/api/_/tickets/[0-9]+/notes|ticketnote|PUT"`)
	httpCmd.Flags().StringVar(&httpData, "data", "", "Body")
	httpCmd.Flags().StringSliceVar(&httpHeaders, "header", []string{}, "Add custom header to the request")
	httpCmd.Flags().StringSliceVar(&httpProxyHeaders, "proxy-header", []string{}, "Headers to add to the HTTP proxy communication during CONNECT")
	httpCmd.Flags().BoolVar(&httpInsecure, "insecure", false, "Allow insecure server connections when using SSL")
	httpCmd.Flags().StringSliceVar(&rootCAs, "rootca", []string{}, "Add root CAs to add to client trust store")
	httpCmd.Flags().StringVar(&tlsServerName, "tls-server-name", "", "TLS server name to send in ClientHello SNI extension")
	httpCmd.Flags().BoolVar(&httpNoKeepalive, "no-keepalive", false, "Disable TCP connection reuse for http connections")
}
