package grpc

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/pretty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	reflectpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
)

var grpcClientShare *clientShare

func init() {
	grpcClientShare = newClientShare()
}

type Generator struct {
	o          GeneratorOptions
	clientConn *grpc.ClientConn
	refClient  *grpcreflect.Client
	descSource grpcurl.DescriptorSource
	formatter  grpcurl.Formatter
	ctx        context.Context
	stats      *stats.Stats
	log        *logrus.Entry
}

type GeneratorOptions struct {
	Target               string
	Method               string
	Headers              []string
	Authority            string
	Data                 string
	Proto                []string
	ImportPath           []string
	Deadline             time.Duration
	MaxConcurrentStreams int
	Unix                 bool
	Plaintext            bool
	Insecure             bool
	CaCert               string
	ClientCert           string
	ClientKey            string
	TlsServerName        string
	DiscardResponse      bool
	Template             bool
	EnableLoadBalancer   bool
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{}
}

func NewGenerator(id int, options GeneratorOptions, ctx context.Context, s *stats.Stats, requestrate int) *Generator {
	log := logrus.WithFields(logrus.Fields{"Id": id})
	return &Generator{o: options, ctx: ctx, stats: s, log: log}
}

func (g *Generator) Init() error {
	var err error

	// Prefer to use proto file if provided. If not, fallback to server reflection
	if len(g.o.Proto) > 0 {
		if len(g.o.ImportPath) == 0 {
			for _, f := range g.o.Proto {
				if filepath.IsAbs(f) {
					g.o.ImportPath = append(g.o.ImportPath, filepath.Dir(f))
				}
			}
		}

		g.descSource, err = grpcurl.DescriptorSourceFromProtoFiles(g.o.ImportPath, g.o.Proto...)
		if err != nil {
			return fmt.Errorf("failed to process proto source files: %v", err)
		}
		g.log.Debugf("grpc descriptor source: %v", g.descSource)

		// For generating cli grpc call template, we don't need to
		// connect to server if proto is explicitly provided
		if !g.o.Template {
			g.clientConn, err = grpcClientShare.getClient(g.o, g.ctx)
			if err != nil {
				return err
			}
		}
	} else {
		g.clientConn, err = grpcClientShare.getClient(g.o, g.ctx)
		if err != nil {
			return err
		}

		md := grpcurl.MetadataFromHeaders(g.o.Headers)
		refCtx := metadata.NewOutgoingContext(g.ctx, md)
		g.refClient = grpcreflect.NewClient(refCtx, reflectpb.NewServerReflectionClient(g.clientConn))
		g.descSource = grpcurl.DescriptorSourceFromServer(g.ctx, g.refClient)
		g.log.Debugf("grpc descriptor source from reflection: %v", g.descSource)
	}

	if !g.o.DiscardResponse {
		_, g.formatter, err = grpcurl.RequestParserAndFormatter(grpcurl.Format("json"), g.descSource, nil, grpcurl.FormatOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *Generator) InitDone() error {
	return nil
}

func (g *Generator) Tick() error {
	_, err := g.Do(g.o.Method, g.o.Data, g.o.Headers)
	if err != nil {
		g.log.Errorf("grpc error: %v", err)
	}
	return nil
}

func (g *Generator) Finish() error {
	return nil
}

func (g *Generator) Template() (string, error) {
	svcs, err := g.descSource.ListServices()
	if err != nil {
		return "", fmt.Errorf("failed to list services: %v", err)
	}

	if len(svcs) == 0 {
		return "", fmt.Errorf("could not find any services")
	}

	cmdline := new(bytes.Buffer)

	for _, s := range svcs {
		cmdline.WriteString(fmt.Sprintf("\n\n========= %v =========\n", s))

		if s[0] == '.' {
			s = s[1:]
		}

		dsc, err := g.descSource.FindSymbol(s)
		if err != nil {
			logrus.Errorf("Failed to resolve symbol %v: %v ", s, err)
			continue
		}

		sd, ok := dsc.(*desc.ServiceDescriptor)
		if !ok {
			logrus.Errorf("Not a service, ignoring: %v\n", s)
			continue
		}

		methods := sd.GetMethods()
		for _, method := range methods {
			cmdline.WriteString(fmt.Sprintf("\n--method '%v'\n", method.GetFullyQualifiedName()))

			md := method.GetInputType()
			tmpl := grpcurl.MakeTemplate(md)
			_, formatter, err := grpcurl.RequestParserAndFormatterFor(grpcurl.Format("json"), g.descSource, true, false, nil)
			if err != nil {
				return "", fmt.Errorf("failed to construct formatter: %v", err)
			}
			str, err := formatter(tmpl)
			if err != nil {
				return "", fmt.Errorf("failed to print template for message %s: %v", s, err)
			}
			c := pretty.Ugly([]byte(str))
			cmdline.WriteString(fmt.Sprintf("--data '%s'\n", string(c)))
		}
	}

	return cmdline.String(), nil
}

type grpcEventHandler struct {
	t               stats.TraceInfo
	sendHeaders     time.Time
	receiveHeaders  time.Time
	receiveResponse time.Time
	msg             string
	formatter       grpcurl.Formatter
}

func (h *grpcEventHandler) OnResolveMethod(md *desc.MethodDescriptor) {
	h.t.Subkey = md.GetFullyQualifiedName()
	//log.Debugf("OnResolveMethod")
}

func (h *grpcEventHandler) OnSendHeaders(md metadata.MD) {
	h.sendHeaders = time.Now()
	//log.Debugf("OnSendHeaders")
}

func (h *grpcEventHandler) OnReceiveHeaders(md metadata.MD) {
	h.receiveHeaders = time.Now()
	//log.Debugf("OnReceiveHeaders: %+v", md)
}

func (h *grpcEventHandler) OnReceiveResponse(resp protoiface.MessageV1) {
	h.receiveResponse = time.Now()
	if h.formatter != nil {
		var err error
		h.msg, err = h.formatter(resp)
		if err != nil {
			logrus.Warnf("Error decoding response: %v", err)
		} else {
			logrus.Debugf("Response: %v", h.msg)
		}
	}
}

func (h *grpcEventHandler) OnReceiveTrailers(stat *status.Status, md metadata.MD) {
	now := time.Now()

	//logrus.Debugf("OnReceiveTrailers : code=%v message=%v", stat.Code(), stat.Message())

	h.t.Error = (stat.Code() != codes.OK)

	switch stat.Code() {
	case codes.OK:
		h.t.Total = now.Sub(h.sendHeaders)
		h.t.Error = false
		return
	case codes.DeadlineExceeded:
		h.t.Error = true
		h.t.DeadlineExceeded = true
		return
	case codes.Canceled:
		// Client side cancelation we will ignore (like Ctrl-C)
		if stat.Message() == "context canceled" {
			h.t.Error = false
		}
		return
	case codes.Unavailable:
		logrus.Warnf("Server unavailable: %v", stat.Message())
		return
	default:
		logrus.Warnf("Error: code=%v: message=%v", stat.Code(), stat.Message())
		return
	}
}

func (g *Generator) Do(method string, data string, headers []string) (string, error) {
	ctx := g.ctx

	if method == "" {
		return "", fmt.Errorf("GRPC: Method name not given")
	}

	if g.o.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.o.Deadline)
		defer cancel()
	}

	h := &grpcEventHandler{}

	if !g.o.DiscardResponse {
		h.formatter = g.formatter
	}

	h.t.Key = g.o.Target

	// We should handle multiple messages?
	// TODO: Don't g.getReq everytime
	reqSupplier, err := g.getReq(g.descSource, data)
	if err != nil {
		return "", err
	}

	err = grpcurl.InvokeRPC(ctx, g.descSource, g.clientConn, method, headers, h, reqSupplier)
	if err != nil {
		return "", err
	}

	h.t.Type = stats.GrpcTrace
	g.stats.RecordMetric(&h.t)

	return h.msg, err
}

func (g *Generator) getReq(descSource grpcurl.DescriptorSource, data string) (grpcurl.RequestSupplier, error) {
	in := strings.NewReader(data)

	includeSeparators := true
	reqParser, _, err := grpcurl.RequestParserAndFormatterFor(grpcurl.Format("json"), descSource, true, includeSeparators, in)
	if err != nil {
		return nil, err
	}

	return reqParser.Next, nil
}

type connInfo struct {
	conn    *grpc.ClientConn
	current int
}

type clientShare struct {
	mux sync.Mutex
	c   map[string]*connInfo
}

func newClientShare() *clientShare {
	return &clientShare{c: make(map[string]*connInfo)}
}

func (g *clientShare) getClient(o GeneratorOptions, ctx context.Context) (*grpc.ClientConn, error) {
	g.mux.Lock()
	defer g.mux.Unlock()

	logrus.Debugf("Getting grpc client for: %v", g.c[o.Target])

	if c, ok := g.c[o.Target]; ok {
		if c.conn != nil && c.current < o.MaxConcurrentStreams {
			logrus.Debugf("Reusing connection for: %v", o.Target)
			c.current++
			return c.conn, nil
		}
	}

	var err error
	c := &connInfo{}
	c.conn, err = g.dial(o, ctx)
	c.current = 1

	g.c[o.Target] = c

	return c.conn, err
}

func (g *clientShare) dial(o GeneratorOptions, ctx context.Context) (*grpc.ClientConn, error) {
	dialTime := 10 * time.Second

	connectTimeout := 10.0
	keepaliveTime := 0.0

	if connectTimeout > 0 {
		dialTime = time.Duration(connectTimeout * float64(time.Second))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTime)
	defer cancel()
	var opts []grpc.DialOption
	if keepaliveTime > 0 {
		timeout := time.Duration(keepaliveTime * float64(time.Second))
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    timeout,
			Timeout: timeout,
		}))
	}

	if o.Authority != "" {
		opts = append(opts, grpc.WithAuthority(o.Authority))
	}

	// if *maxMsgSz > 0 {
	// 	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(*maxMsgSz)))
	// }

	o.Plaintext = true
	var creds credentials.TransportCredentials
	if !o.Plaintext {
		var err error
		creds, err = grpcurl.ClientTransportCredentials(o.Insecure, o.CaCert, o.ClientCert, o.ClientKey)
		if err != nil {
			return nil, err
		}
		if o.TlsServerName != "" {
			if err := creds.OverrideServerName(o.TlsServerName); err != nil {
				return nil, err
			}
		}
	}

	network := "tcp"
	if o.Unix {
		network = "unix"
	}

	target := o.Target
	if o.EnableLoadBalancer {
		opts = append(opts, grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
		if !strings.Contains(target, "://") {
			target = "dns:///" + target
		}
	}

	opts = append(opts, grpc.WithUserAgent("load-generator"))

	return grpcurl.BlockingDial(ctx, network, target, creds, opts...)
}
