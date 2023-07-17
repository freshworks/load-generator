package cql

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	gocqlastra "github.com/datastax/gocql-astra"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/gocql/gocql"
	"github.com/sirupsen/logrus"
	lua "github.com/yuin/gopher-lua"
	luajson "layeh.com/gopher-json"
)

const defaultConnectTimeout = 5000 * time.Millisecond
const defaultWriteTimeout = 5000 * time.Millisecond
const defaultConsistency = gocql.LocalQuorum

type Generator struct {
	Session *gocql.Session
	o       GeneratorOptions
	log     *logrus.Entry
	ctx     context.Context
	stats   *stats.Stats
	query   *gocql.Query
	hostKey string
}

type GeneratorOptions struct {
	resolvedTargets   []string
	Targets           []string
	Query             string
	Keyspace          string
	Consistency       string
	Username          string
	Password          string
	Plaintext         bool
	Insecure          bool
	CaCert            string
	ClientCert        string
	ClientKey         string
	TlsServerName     string
	AstraSecureBundle string
	// Do not use node information from system.peers table, just use the
	// manually supplied ones
	DisablePeersLookup bool
	IgnorePeerAddr     bool

	ConnectTimeout    time.Duration
	WriteTimeout      time.Duration
	NumConnsPerHost   int
	EnableCompression bool
	NumRetries        int
	// TODO: configure it
	ReconnectionPolicy    string
	MaxPreparedStmts      int
	SerialConsistency     string
	HostSelectionPolicy   string
	DCName                string
	WriteCoalesceWaitTime time.Duration
	TrackMetricsPerNode   bool
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{}
}

func NewGenerator(id int, o GeneratorOptions, ctx context.Context, requestrate int, s *stats.Stats) *Generator {
	log := logrus.WithFields(logrus.Fields{"Id": id})

	g := &Generator{
		ctx:     ctx,
		log:     log,
		o:       o,
		stats:   s,
		hostKey: o.sessionKey(),
	}

	sess, err := getCqlSession(g)
	if err != nil {
		panic(err)
	}

	g.Session = sess

	return g
}

func (g *Generator) Init() error {
	g.query = g.Session.Query(g.o.Query).WithContext(g.ctx)
	return nil
}

func (g *Generator) InitDone() error { return nil }

func (g *Generator) ObserveQuery(ctx context.Context, oq gocql.ObservedQuery) {
	// g.log.Infof("query observer: %v", oq)
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.CqlTrace
	traceInfo.Key = g.hostKey
	traceInfo.Subkey = oq.Statement
	if oq.Err != nil && !errors.Is(oq.Err, context.Canceled) {
		traceInfo.Error = true
	}
	if !traceInfo.Error {
		traceInfo.Total = oq.End.Sub(oq.Start)
	}

	if g.o.TrackMetricsPerNode {
		traceInfo2 := traceInfo
		traceInfo2.Key = net.JoinHostPort(oq.Host.ConnectAddress().String(), strconv.Itoa(oq.Host.Port()))
		g.stats.RecordMetric(&traceInfo2)
	}

	g.stats.RecordMetric(&traceInfo)
}

func (g *Generator) ObserveConnect(oc gocql.ObservedConnect) {
	if oc.Err != nil {
		g.log.Errorf("error connecting: %v (hostinfo=%v)", oc.Err, oc.Host.String())
		return
	}
	g.log.Debugf("connected, duration=%s hostinfo=%s", oc.End.Sub(oc.Start), oc.Host.String())
}

func (g *Generator) Tick() error {
	return g.query.Exec()
}

func (g *Generator) Finish() error {
	return nil
}

// set<bigint>
func (g *Generator) AsSetInt(c lua.LValue) ([]int, error) {
	// TODO: temp hack, we convert lua table to json, then convert it
	// back to go []interface{}. Convert this to use
	// reflection
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []int
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (g *Generator) AsTimestamp(i int64) time.Time {
	return time.Unix(i, 0)
}

// set<text>
func (g *Generator) AsSetText(c lua.LValue) ([]string, error) {
	// TODO: temp hack, we convert lua table to json, then convert it
	// back to go []interface{}. Convert this to use
	// reflection
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []string
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// map<text, bigint>
func (g *Generator) AsMapInt(c lua.LValue) (interface{}, error) {
	// TODO: temp hack, we convert lua table to json, then convert it
	// back to go []map[string]interface{}. Convert this to use
	// reflection
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []map[string]int
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// map<text, boolean>
func (g *Generator) AsMapBool(c lua.LValue) (interface{}, error) {
	// TODO: temp hack, we convert lua table to json, then convert it
	// back to go []map[string]interface{}. Convert this to use
	// reflection
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []map[string]bool
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// map<text, boolean>
func (g *Generator) AsMapTimestamp(c lua.LValue) (interface{}, error) {
	// TODO: temp hack, we convert lua table to json, then convert it
	// back to go []map[string]interface{}. Convert this to use
	// reflection
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []map[string]time.Time
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// map<text, float>
func (g *Generator) AsMapFloat(c lua.LValue) (interface{}, error) {
	// TODO: temp hack, we convert lua table to json, then convert it
	// back to go []map[string]interface{}. Convert this to use
	// reflection
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []map[string]float64
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// map<string, set<text>>
func (g *Generator) AsMapSetText(c lua.LValue) (interface{}, error) {
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []map[string]interface{}
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// map<string, set<bigint>>
func (g *Generator) AsMapSetInt(c lua.LValue) (interface{}, error) {
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []map[string][]int
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// set<map<x, x>>
func (g *Generator) AsSetMap(c lua.LValue) (interface{}, error) {
	// TODO: temp hack, we convert lua table to json, then convert it
	// back to go []map[string]interface{}. Convert this to use
	// reflection
	r, err := luajson.Encode(c)
	if err != nil {
		return nil, err
	}
	var out []map[string]interface{}
	err = json.Unmarshal(r, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// We will store the sessions per target, as it is possible to hit multiple
// cassandra clusters (via lua scripts)
var cqlSessionsMux sync.Mutex
var cqlSessions = map[string]*gocql.Session{}

func (o *GeneratorOptions) sessionKey() string {
	if o.AstraSecureBundle != "" {
		return o.AstraSecureBundle
	}

	return strings.Join(o.Targets, "+")
}

func getCqlSession(g *Generator) (*gocql.Session, error) {
	// TODO: do fine grained locking
	cqlSessionsMux.Lock()
	defer cqlSessionsMux.Unlock()

	o := g.o

	sessKey := o.sessionKey()
	v, ok := cqlSessions[sessKey]
	if ok {
		return v, nil
	}

	var cluster *gocql.ClusterConfig
	var err error
	if o.AstraSecureBundle != "" {
		ct := 10 * time.Second
		if o.ConnectTimeout != 0 {
			ct = o.ConnectTimeout
		}
		cluster, err = gocqlastra.NewClusterFromBundle(o.AstraSecureBundle, o.Username, o.Password, ct)
		if err != nil {
			return nil, err
		}
	} else {
		// resolve targets to IPs because gocql library is prone to create imbalanced connection pool when DNS is passed.
		// https://github.com/gocql/gocql/issues/1575
		if err := g.resolveTargets(); nil != err {
			return nil, err
		}
		cluster = gocql.NewCluster(g.o.resolvedTargets...)
		if !o.Plaintext {
			var s tls.Config

			s.InsecureSkipVerify = o.Insecure
			if o.TlsServerName != "" {
				s.ServerName = o.TlsServerName
			}

			// TODO handle root CAs etc
			// s.ClientCAs *x509.CertPool
			// s.ClientAuth ClientAuthType
			// s.Certificates []Certificate

			//               o.ClientCert
			//                 o.ClientKey
			//                 o.CaCert

			cluster.SslOpts = &gocql.SslOptions{
				Config: &s,
			}
		}
	}

	cluster.Keyspace = o.Keyspace

	cluster.Logger = &logger{log: logrus.WithField("sess-key", sessKey)}

	if o.ConnectTimeout != 0 {
		cluster.Timeout = o.ConnectTimeout
		cluster.ConnectTimeout = o.ConnectTimeout
	} else {
		cluster.Timeout = defaultConnectTimeout
		cluster.ConnectTimeout = defaultConnectTimeout
	}
	if o.WriteTimeout != 0 {
		cluster.WriteTimeout = o.ConnectTimeout
	} else {
		cluster.WriteTimeout = defaultWriteTimeout
	}

	if o.Consistency != "" {
		cluster.Consistency = gocql.ParseConsistency(o.Consistency)
	} else {
		cluster.Consistency = defaultConsistency
	}

	cluster.DisableInitialHostLookup = o.DisablePeersLookup

	cluster.IgnorePeerAddr = o.IgnorePeerAddr

	if o.EnableCompression {
		cluster.Compressor = &gocql.SnappyCompressor{}
	}

	if o.NumConnsPerHost != 0 {
		cluster.NumConns = o.NumConnsPerHost
	}

	if o.NumRetries != 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: o.NumRetries}
	}

	if o.MaxPreparedStmts != 0 {
		cluster.MaxPreparedStmts = o.MaxPreparedStmts
	}

	if o.HostSelectionPolicy != "" {
		switch o.HostSelectionPolicy {
		case "RoundRobin":
			cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
		case "DCAwareRoundRobin":
			if o.DCName == "" {
				return nil, fmt.Errorf("DCAwareRoundRobin policy needs datacenter name, not provided")
			}
			cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(o.DCName)
		case "TokenAwareWithRoundRobinFallback":
			cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
		case "TokenAwareWithDCAwareRoundRobinFallback":
			if o.DCName == "" {
				panic("TokenAwareWithDCAwareRoundRobinFallback policy needs datacenter name, not provided")
			}
			cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(o.DCName))
		default:
			return nil, fmt.Errorf("unknown host selection policy: %v", o.HostSelectionPolicy)
		}
	}

	if o.WriteCoalesceWaitTime != 0 {
		cluster.WriteCoalesceWaitTime = o.WriteCoalesceWaitTime
	} else {
		cluster.WriteCoalesceWaitTime = 0
	}

	if o.Keyspace != "" {
		cluster.Keyspace = o.Keyspace
	}

	if o.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: o.Username,
			Password: o.Password,
		}
	}

	cluster.QueryObserver = g
	cluster.ConnectObserver = g

	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	cqlSessions[sessKey] = sess

	return sess, nil
}

type logger struct {
	log *logrus.Entry
}

func (l *logger) Print(v ...interface{}) {
	l.log.Debug(v...)
}

func (l *logger) Printf(format string, v ...interface{}) {
	l.log.Debugf(format, v...)
}

func (l *logger) Println(v ...interface{}) {
	l.log.Debugln(v...)
}

func (g *Generator) resolveTargets() error {
	var parsedTargets []string
	for _, target := range g.o.Targets {
		var filtered_target, port string

		filtered_target, port, err := net.SplitHostPort(target)

		if nil != err {
			return fmt.Errorf("target %v seems to be malformed: %v", target, err.Error())
		}

		if nil != net.ParseIP(filtered_target) {
			// original target is an IP, use the target as it is
			parsedTargets = append(parsedTargets, target)
			continue
		}
		g.log.Debugf("resolving DNS: %v", filtered_target)
		t, err := resolveDNS(filtered_target)
		if nil != err {
			return fmt.Errorf("unable to resolve DNS: %v", filtered_target)
		}
		g.log.Debugf("appending targets: %v", t)
		// append port suffix to all resolved IPs
		t = addPortSuffix(t, port)
		parsedTargets = append(parsedTargets, t...)
	}
	g.log.Infof("resolved targets: %v from: %v", parsedTargets, g.o.Targets)
	g.o.resolvedTargets = parsedTargets
	return nil
}

func resolveDNS(dns string) ([]string /* list of IPs */, error) {
	var ips []string
	netIPs, err := net.LookupIP(dns)
	if err != nil {
		return nil, err
	}
	for _, netIP := range netIPs {
		ips = append(ips, netIP.String())
	}
	return ips, nil
}

func addPortSuffix(ips []string, port string) []string {
	var targets []string
	for _, ip := range ips {
		targets = append(targets, ip+":"+port)
	}
	return targets
}
