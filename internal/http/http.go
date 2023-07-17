package http

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	awsdefaults "github.com/aws/aws-sdk-go/aws/defaults"
	awssigner "github.com/aws/aws-sdk-go/aws/signer/v4"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/publicsuffix"
)

var tlsKeyLogWriter io.Writer

type Generator struct {
	Headers   map[string]string
	log       *logrus.Entry
	url       string
	client    *http.Client
	options   GeneratorOptions
	ctx       context.Context
	stats     *stats.Stats
	awsSigner *awssigner.Signer
}

type GeneratorOptions struct {
	Url                 url.URL
	Data                string
	DiscardResponse     bool
	StreamResponse      bool
	KeepAlive           bool
	Method              string
	Headers             map[string]string
	ProxyHeaders        map[string]string
	Insecure            bool
	AggregateMethodPath map[string]map[*regexp.Regexp]string
	TlsServerName       string
	RootCAs             []string
	DisableCompression  bool
	DisableCookieJar    bool
	AwsSign             bool
	AwsSignInfo         AwsSignInfo
}

type AwsSignInfo struct {
	Region  string
	Service string
}

func init() {
	sslkeylog, ok := os.LookupEnv("SSLKEYLOGFILE")
	if ok && sslkeylog != "" {
		var e error
		tlsKeyLogWriter, e = os.OpenFile(sslkeylog, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if e != nil {
			logrus.Warnf("Failed to open keylog file: %v", e)
		}
	}

}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{
		KeepAlive:           true,
		Headers:             map[string]string{},
		ProxyHeaders:        map[string]string{},
		AggregateMethodPath: map[string]map[*regexp.Regexp]string{},
	}
}

func NewGenerator(id int, o GeneratorOptions, ctx context.Context, requestrate int, s *stats.Stats) *Generator {
	log := logrus.WithFields(logrus.Fields{"Id": id})

	proxyHdr := http.Header{}
	for k, v := range o.ProxyHeaders {
		proxyHdr.Set(k, v)
	}

	tc := &tls.Config{
		InsecureSkipVerify: o.Insecure,
		KeyLogWriter:       tlsKeyLogWriter,
	}

	if len(o.RootCAs) > 0 {
		tc.RootCAs = x509.NewCertPool()

		for _, f := range o.RootCAs {
			certs, err := ioutil.ReadFile(f)
			if err != nil {
				log.Fatal(err)
			}
			ok := tc.RootCAs.AppendCertsFromPEM(certs)
			if !ok {
				log.Fatal("cannot append certs to root CA")
			}
		}
	}

	if o.TlsServerName != "" {
		tc.ServerName = o.TlsServerName
	}

	// A cookie jar to hold the cookies
	var jar http.CookieJar
	if !o.DisableCookieJar {
		var err error
		jar, err = cookiejar.New(&cookiejar.Options{
			PublicSuffixList: publicsuffix.List,
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	var awsSigner *awssigner.Signer
	if o.AwsSign {
		awsSigner = awssigner.NewSigner(awsdefaults.Get().Config.Credentials)
	}

	return &Generator{
		ctx:       ctx,
		log:       log,
		Headers:   make(map[string]string),
		options:   o,
		url:       o.Url.String(),
		stats:     s,
		awsSigner: awsSigner,
		client: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
			// TODO: Tweak defaults properly for high concurrent connections
			Transport: &http.Transport{
				Proxy:              http.ProxyFromEnvironment,
				ProxyConnectHeader: proxyHdr,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:          0,
				MaxIdleConnsPerHost:   2 * requestrate,
				MaxConnsPerHost:       0,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				DisableKeepAlives:     !o.KeepAlive,
				TLSClientConfig:       tc,
				DisableCompression:    o.DisableCompression,
			},
			Jar: jar,
		}}
}

func (g *Generator) Init() error {
	return nil
}

func (g *Generator) InitDone() error { return nil }

func (g *Generator) Tick() error {
	_, err := g.Do(g.options.Method, g.url, nil, g.options.Data)
	if err != nil {
		g.log.Errorf("http error: %v", err)
	}

	return nil
}

func (g *Generator) Finish() error { return nil }

func (g *Generator) Do(method string, Url string, headers map[string]string,
	body string) (*http.Response, error) {

	return g.do(method, Url, headers, body)
}

func (g *Generator) SetTlsServerName(n string) {
	g.client.Transport.(*http.Transport).TLSClientConfig.ServerName = n
}

func (g *Generator) CloseIdleConnections() {
	g.client.CloseIdleConnections()
}

func (g *Generator) DoFormUrl(method string, Url string, headers map[string]string,
	params map[string][]string) (*http.Response, error) {
	var body string

	if headers == nil {
		headers = make(map[string]string, 1)
	}

	headers["Content-Type"] = "application/x-www-form-urlencoded"
	body, _ = g.CreateFormUrlEncoded(params)

	return g.do(method, Url, headers, body)
}

func (g *Generator) DoFormMultipart(method string, Url string, headers map[string]string,
	params map[string][]string) (*http.Response, error) {
	var body, boundary string
	var err error

	if headers == nil {
		headers = make(map[string]string, 1)
	}

	body, boundary, err = g.CreateFormMultiPart(params)
	if err != nil {
		return nil, err
	}
	headers["Content-Type"] = "multipart/form-data; boundary=" + boundary

	return g.do(method, Url, headers, body)
}

func (g *Generator) do(method string, urlStr string, headers map[string]string, body string) (*http.Response, error) {
	var Url *url.URL
	Url, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	Url.RawQuery = Url.Query().Encode()

	var startTime, endTime time.Time
	trace := &httptrace.ClientTrace{
		// GetConn is called before connection creation or retrieval
		// from the connection pool. This will also include the TLS
		// session setup time. "GotConn" is called after the
		// connection is established/retrieved from pool, which will
		// not include TLS session setup.
		//
		// So for keep-alive connections, there might not be a big
		// difference between GetConn and GotConn, but for HTTPS
		// requests, if keep-alive is disabled, there will be a
		// difference in timings
		GetConn: func(hostPort string) {
			startTime = time.Now()
		},
	}

	ctx := httptrace.WithClientTrace(g.ctx, trace)

	req, err := http.NewRequestWithContext(ctx, method, Url.String(), strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("error: %s", err)
	}

	// Add global headers
	for k, v := range g.options.Headers {
		req.Header.Set(k, v)
	}

	// Add per instance headers
	for k, v := range g.Headers {
		req.Header.Set(k, v)
	}

	// Add headers passed to this function
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	if method == "POST" || method == "PUT" {
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}

	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
	}

	if g.options.AwsSign {
		_, err := g.awsSigner.Sign(req, strings.NewReader(body), g.options.AwsSignInfo.Service, g.options.AwsSignInfo.Region, time.Now())
		if err != nil {
			return nil, err
		}
	}

	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.HttpTrace
	traceInfo.Key = fmt.Sprintf("%s://%s", req.URL.Scheme, req.URL.Host)
	traceInfo.Subkey = req.URL.Path

	resp, err := g.client.Do(req)
	if err != nil {
		traceInfo.Error = true
		g.stats.RecordMetric(&traceInfo)
		return nil, err
	}

	if resp != nil {
		if resp.Body != nil {
			if !g.options.StreamResponse {
				if g.options.DiscardResponse {
					io.Copy(ioutil.Discard, resp.Body)
					resp.Body.Close()
				} else {
					// TODO: Handle large responses, we could run out of
					// memory with the current method.

					// Make a copy of the response
					b, _, err := copyReader(resp.Body, resp.ContentLength)
					if err != nil {
						g.log.Warnf("Failed to read the response body: %v", err)
					}
					resp.Body.Close()
					resp.Body = b
				}
			}
		}

		// End time must be after we read the response
		endTime = time.Now()

		traceInfo.Total = endTime.Sub(startTime)
		traceInfo.Status = resp.StatusCode

		if len(g.options.AggregateMethodPath) > 0 {
			for k, v := range g.options.AggregateMethodPath[req.Method] {
				if k.MatchString(req.URL.Path) {
					traceInfo.Subkey = k.ReplaceAllString(req.URL.Path, v)
					break
				}
			}

			if traceInfo.Subkey == "" {
				for k, v := range g.options.AggregateMethodPath["any"] {
					if k.MatchString(req.URL.Path) {
						traceInfo.Subkey = k.ReplaceAllString(req.URL.Path, v)
						break
					}
				}
			}
		} else {
			traceInfo.Subkey = req.URL.Path
		}

		g.stats.RecordMetric(&traceInfo)
	}

	return resp, nil
}

// Url encoded form
func (g *Generator) CreateFormUrlEncoded(formFields map[string][]string) (string, error) {
	var form url.Values = formFields
	return form.Encode(), nil
}

// Multi-part form
func (g *Generator) CreateFormMultiPart(formFields map[string][]string) (body, boundary string, err error) {
	var b bytes.Buffer
	var fw io.Writer

	w := multipart.NewWriter(&b)
	for k, v := range formFields {
		for _, v2 := range v {
			if strings.HasPrefix(v2, "@") {
				// TODO: Content-Type will be set to
				// octet-stream, need to handle it using
				// w.CreatePart API.
				var part io.Writer
				part, err = w.CreateFormFile(k, filepath.Base(v2))
				if err != nil {
					return
				}

				var file *os.File
				file, err = os.Open(v2[1:])
				if err != nil {
					return
				}
				defer file.Close()

				_, err = io.Copy(part, file)
				if err != nil {
					return
				}
			} else {

				if fw, err = w.CreateFormField(k); err != nil {
					return
				}

				if _, err = fw.Write([]byte(string(v2))); err != nil {
					return
				}
			}
		}
	}

	w.Close()

	body = b.String()
	boundary = w.Boundary()
	err = nil

	return
}

func (g *Generator) GetCookies(url string) ([]*http.Cookie, error) {
	if g.client.Jar != nil {
		u, e := g.options.Url.Parse(url)
		if e != nil {
			return nil, e
		}
		return g.client.Jar.Cookies(u), nil
	}

	return nil, errors.New("cookie jar is disabled")
}

func copyReader(r io.ReadCloser, capacity int64) (res io.ReadCloser, len int64, err error) {

	if capacity <= 0 {
		capacity = 512
	}

	// Copied from ioutil.ReadAll
	buf := bytes.NewBuffer(make([]byte, 0, capacity))
	// If the buffer overflows, we will get bytes.ErrTooLarge.
	// Return that as an error. Any other panic remains.
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok && panicErr == bytes.ErrTooLarge {
			err = panicErr
		} else {
			panic(e)
		}
	}()

	len, err = buf.ReadFrom(r)
	if err == nil {
		br := bytes.NewReader(buf.Bytes())
		res = ioutil.NopCloser(bufio.NewReader(br))
	}

	return res, len, err
}
