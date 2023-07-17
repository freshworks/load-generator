package grpc

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/reflection"
)

func setupGRPCSvr(t *testing.T) (*grpc.Server, int) {
	svr := grpc.NewServer()
	grpc_testing.RegisterTestServiceServer(svr, testServer{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.Nil(t, err)

	reflection.Register(svr)

	port := l.Addr().(*net.TCPAddr).Port
	go svr.Serve(l)

	return svr, port
}

func TestGRPC(t *testing.T) {
	// 	sourceProtoFiles, err := DescriptorSourceFromProtoFiles(nil, protoFile)
	// require.Nil(t, err)

	// _ = sourceProtoFiles

	require := require.New(t)
	assert := assert.New(t)

	svr, port := setupGRPCSvr(t)
	defer svr.Stop()

	options := NewOptions()
	options.Plaintext = true
	options.Target = fmt.Sprintf("127.0.0.1:%d", port)
	options.Proto = []string{"testing/test.proto"}
	options.Method = "grpc.testing.TestService.UnaryCall"

	sts := stats.New("id", 1, 1, 0, false)
	sts.Start()
	defer sts.Stop()

	logger, _ := test.NewNullLogger()
	require.NotNil(logger)

	t.Run("CommandlineTemplate", func(t *testing.T) {
		t.Run("Using Proto File", func(t *testing.T) {
			o := NewOptions()
			o.Proto = []string{"testing/test.proto"}
			o.Template = true

			g := NewGenerator(0, *o, context.Background(), sts, 1)
			require.NotNil(g)

			err := g.Init()
			require.Nil(err)

			str, err := g.Template()
			require.Nil(err)

			require.Contains(str, `--method 'grpc.testing.TestService.EmptyCall'`)
			require.Contains(str, `--method 'grpc.testing.TestService.UnaryCall'`)
			require.Contains(str, `--data '{"responseType":"COMPRESSABLE","responseSize":0,"payload":{"type":"COMPRESSABLE","body":""},"fillUsername":false,"fillOauthScope":false,"responseStatus":{"code":0,"message":""}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.StreamingOutputCall'`)
			require.Contains(str, `--data '{"responseType":"COMPRESSABLE","responseParameters":[{"size":0,"intervalUs":0}],"payload":{"type":"COMPRESSABLE","body":""},"responseStatus":{"code":0,"message":""}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.StreamingInputCall'`)
			require.Contains(str, `--data '{"payload":{"type":"COMPRESSABLE","body":""}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.FullDuplexCall'`)
			require.Contains(str, `--data '{"responseType":"COMPRESSABLE","responseParameters":[{"size":0,"intervalUs":0}],"payload":{"type":"COMPRESSABLE","body":""},"responseStatus":{"code":0,"message":""}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.HalfDuplexCall'`)
			require.Contains(str, `--data '{"responseType":"COMPRESSABLE","responseParameters":[{"size":0,"intervalUs":0}],"payload":{"type":"COMPRESSABLE","body":""},"responseStatus":{"code":0,"message":""}}'`)
			require.Contains(str, `--method 'grpc.testing.UnimplementedService.UnimplementedCall'`)
			require.Contains(str, `--data '{}'`)
		})

		t.Run("Using Server Reflection", func(t *testing.T) {
			o := NewOptions()
			o.Template = true
			o.Plaintext = true
			o.Target = fmt.Sprintf("127.0.0.1:%d", port)

			g := NewGenerator(0, *o, context.Background(), sts, 1)
			require.NotNil(g)

			err := g.Init()
			require.Nil(err)

			str, err := g.Template()
			require.Nil(err)

			require.Contains(str, `--method 'grpc.testing.TestService.EmptyCall'`)
			require.Contains(str, `--method 'grpc.testing.TestService.UnaryCall'`)
			require.Contains(str, `--data '{"responseType":"COMPRESSABLE","responseSize":0,"payload":{"type":"COMPRESSABLE","body":""},"fillUsername":false,"fillOauthScope":false,"responseCompressed":{"value":false},"responseStatus":{"code":0,"message":""},"expectCompressed":{"value":false},"fillServerId":false,"fillGrpclbRouteType":false,"orcaPerQueryReport":{"cpuUtilization":0,"memoryUtilization":0,"requestCost":{"":0},"utilization":{"":0}}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.StreamingOutputCall'`)
			require.Contains(str, `--data '{"responseType":"COMPRESSABLE","responseParameters":[{"size":0,"intervalUs":0,"compressed":{"value":false}}],"payload":{"type":"COMPRESSABLE","body":""},"responseStatus":{"code":0,"message":""},"orcaOobReport":{"cpuUtilization":0,"memoryUtilization":0,"requestCost":{"":0},"utilization":{"":0}}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.StreamingInputCall'`)
			require.Contains(str, `--data '{"payload":{"type":"COMPRESSABLE","body":""},"expectCompressed":{"value":false}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.FullDuplexCall'`)
			require.Contains(str, `--data '{"responseType":"COMPRESSABLE","responseParameters":[{"size":0,"intervalUs":0,"compressed":{"value":false}}],"payload":{"type":"COMPRESSABLE","body":""},"responseStatus":{"code":0,"message":""},"orcaOobReport":{"cpuUtilization":0,"memoryUtilization":0,"requestCost":{"":0},"utilization":{"":0}}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.HalfDuplexCall'`)
			require.Contains(str, `--data '{"responseType":"COMPRESSABLE","responseParameters":[{"size":0,"intervalUs":0,"compressed":{"value":false}}],"payload":{"type":"COMPRESSABLE","body":""},"responseStatus":{"code":0,"message":""},"orcaOobReport":{"cpuUtilization":0,"memoryUtilization":0,"requestCost":{"":0},"utilization":{"":0}}}'`)
			require.Contains(str, `--method 'grpc.testing.TestService.UnimplementedCall'`)
			require.Contains(str, `--data '{}'`)
		})
	})

	t.Run("CheckConnectionNotShared", func(t *testing.T) {
		s := newClientShare()

		o := *options
		o.MaxConcurrentStreams = 1
		c1, err := s.getClient(o, context.Background())
		require.Nil(err)
		require.NotNil(c1)

		c2, err := s.getClient(o, context.Background())
		require.Nil(err)
		require.NotNil(c2)

		assert.NotEqual(c1, c2)
	})

	t.Run("CheckConnectionShared", func(t *testing.T) {
		s := newClientShare()

		o := *options
		o.MaxConcurrentStreams = 2
		c1, err := s.getClient(o, context.Background())
		require.Nil(err)
		require.NotNil(c1)

		c2, err := s.getClient(o, context.Background())
		require.Nil(err)
		require.NotNil(c2)

		assert.Equal(c1, c2)

		c3, err := s.getClient(o, context.Background())
		require.Nil(err)
		require.NotNil(c3)

		assert.NotEqual(c2, c3)
	})

	t.Run("Basic", func(t *testing.T) {
		//logrus.SetLevel(logrus.DebugLevel)
		g := NewGenerator(0, *options, context.Background(), sts, 1)
		require.NotNil(g)

		sts.Reset()

		err := g.Init()
		require.Nil(err)

		err = g.InitDone()
		require.Nil(err)

		err = g.Tick()
		require.Nil(err)
		r := getStatResultFor(sts, options.Target, options.Method)
		require.NotNil(t, r)
		require.Equal(int64(1), r.Histogram.Count)

		err = g.Tick()
		require.Nil(err)
		r = getStatResultFor(sts, options.Target, options.Method)
		require.NotNil(t, r)
		require.Equal(int64(2), r.Histogram.Count)

		err = g.Finish()
		require.Nil(err)
	})
}

func getStatResultFor(s *stats.Stats, key string, subkey string) *stats.Result {
	r := s.Export()

	for _, rr := range r.Results {
		if key == rr.Target && subkey == rr.SubTarget {
			return &rr
		}
	}

	return nil
}

type testServer struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (testServer) UnaryCall(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return &grpc_testing.SimpleResponse{
		Payload: req.Payload,
	}, nil
}
