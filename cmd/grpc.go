package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/freshworks/load-generator/internal/grpc"
	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/spf13/cobra"
)

var grpcCmd = &cobra.Command{
	Use:   "grpc <target>",
	Short: "gRPC load generator",
	Long: `gRPC load generator using protobuf reflection.
Requires just the proto file(s) and payload in json format.
`,
	Example: `
Generate sample gRPC method/payload options:
 If gRPC server reflection is available:
    lg grpc --template --plaintext example.com:50051

 If you want to use .proto files:
    lg grpc --template --proto ./path/to/helloworld/helloworld.proto \
       --import-path ./path/to/helloworld/

Make the gRPC call:
    lg grpc --method 'helloworld.Greeter.SayHello' --data '{"name": "example"}' \
       --plaintext example.com:50051

Make the gRPC call with roundrobin loadbalancer all the available IPs
    lg grpc --method 'helloworld.Greeter.SayHello' --data '{"name": "example"}' \
       --plaintext --enable-load-balancer example.com:50051

Make the gRPC call but use custom DNS resolver
    lg grpc --method 'helloworld.Greeter.SayHello' --data '{"name": "example"}' \
       --plaintext dns://1.1.1.1:53/example.com:50051
`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := grpc.NewOptions()
			if len(args) > 0 {
				o.Target = args[0]
			}
			o.Method = grpcMethod
			o.Headers = grpcHeaders
			o.Authority = grpcAuthority
			o.Data = grpcData
			o.Proto = grpcProtoFiles
			o.ImportPath = grpcProtoImportPaths
			o.Plaintext = grpcPlaintext
			o.Insecure = grpcInsecure
			o.CaCert = grpcCacert
			o.ClientCert = grpcClientcert
			o.ClientKey = grpcClientkey
			o.TlsServerName = grpcTlsServerName
			o.Unix = grpcUnix
			o.Deadline = grpcDeadline
			o.MaxConcurrentStreams = grpcMaxConcurrentStreams
			o.Template = grpcTemplate
			o.EnableLoadBalancer = grpcEnableLoadBalancer

			return grpc.NewGenerator(id, *o, cmd.Context(), s, requestrate)
		}

		if grpcTemplate {
			g := newGenerator(0, 0, 0, cmd.Context(), stats.New("id", 1, 1, 0, false)).(*grpc.Generator)
			err := g.Init()
			if err != nil {
				return err
			}

			r, err := g.Template()
			if err != nil {
				return err
			}
			fmt.Printf("%v", r)
			os.Exit(0)
		}

		if len(args) == 0 {
			return fmt.Errorf("target server address was not given")
		}

		if grpcMethod == "" {
			return fmt.Errorf(`mandatory "method" argument was not given`)
		}
		if grpcData == "" {
			return fmt.Errorf(`mandatory "data" argument was not given`)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var (
	grpcTemplate             bool
	grpcMethod               string
	grpcHeaders              []string
	grpcAuthority            string
	grpcData                 string
	grpcProtoFiles           []string
	grpcProtoImportPaths     []string
	grpcPlaintext            bool
	grpcInsecure             bool
	grpcCacert               string
	grpcClientcert           string
	grpcClientkey            string
	grpcTlsServerName        string
	grpcUnix                 bool
	grpcDeadline             time.Duration
	grpcMaxConcurrentStreams int
	grpcEnableLoadBalancer   bool
)

func init() {
	rootCmd.AddCommand(grpcCmd)

	grpcCmd.Flags().BoolVar(&grpcTemplate, "template", false, "Print commandline template for all services, methods in the given protos and exit")
	grpcCmd.Flags().StringVar(&grpcMethod, "method", "", "GRPC Method")
	grpcCmd.Flags().StringSliceVar(&grpcHeaders, "header", []string{}, "Add custom header to the request")
	grpcCmd.Flags().StringVar(&grpcAuthority, "authority", "", ":authority header in underlying http2 request")
	grpcCmd.Flags().StringVar(&grpcData, "data", "", "Body of the request, in JSON format")
	grpcCmd.Flags().StringSliceVar(&grpcProtoFiles, "proto", []string{}, "Proto file")
	grpcCmd.Flags().StringSliceVar(&grpcProtoImportPaths, "import-path", []string{}, "Proto file import path")
	grpcCmd.Flags().BoolVar(&grpcPlaintext, "plaintext", false, "Use plaintext transport")
	grpcCmd.Flags().BoolVar(&grpcInsecure, "insecure", false, "Allow invalid certificates when using SSL")
	grpcCmd.Flags().StringVar(&grpcCacert, "cacert", "", "File containing trusted root certificate for verifying server")
	grpcCmd.Flags().StringVar(&grpcClientcert, "client-cert", "", "File containing client public certificate")
	grpcCmd.Flags().StringVar(&grpcClientkey, "client-key", "", "File containing client private key")
	grpcCmd.Flags().StringVar(&grpcTlsServerName, "tls-servername", "", "Servername to use while validating server cetificate name")
	grpcCmd.Flags().BoolVar(&grpcUnix, "unix", false, "Server address is unix domain socket")
	grpcCmd.Flags().DurationVar(&grpcDeadline, "deadline", 0, "grpc deadline")
	grpcCmd.Flags().IntVar(&grpcMaxConcurrentStreams, "max-concurrent-streams", 1, "How many streams to concurrently use in underlying http2 transport")
	grpcCmd.Flags().BoolVar(&grpcEnableLoadBalancer, "enable-load-balancer", false, "Enable roundrobin load balancer")
}
