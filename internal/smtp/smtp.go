package smtp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/emersion/go-sasl"
	"github.com/emersion/go-smtp"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
)

var tlsKeyLogWriter io.Writer

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

type Generator struct {
	log       *logrus.Entry
	o         GeneratorOptions
	ctx       context.Context
	stats     *stats.Stats
	tlsConfig *tls.Config
	client    *smtp.Client
}

type GeneratorOptions struct {
	Target                 string
	Username               string
	Password               string
	From                   string
	To                     string
	Subject                string
	Data                   string
	Plaintext              bool
	Insecure               bool
	TlsServerName          string
	RootCAs                []string
	DisableConnectionReuse bool
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{
		Target: "127.0.0.1:25",
	}
}

func NewGenerator(id int, o GeneratorOptions, ctx context.Context, requestrate int, s *stats.Stats) *Generator {
	log := logrus.WithFields(logrus.Fields{"Id": id})

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

	return &Generator{log: log, o: o, ctx: ctx, stats: s, tlsConfig: tc}
}

func (g *Generator) Init() error {
	c, err := g.getSmtpClient()
	if err != nil {
		return err
	}
	g.client = c
	return nil
}

func (g *Generator) InitDone() error {
	return nil
}

func (g *Generator) Tick() error {
	return g.SendMail(g.o.From, g.o.To, g.o.Subject, g.o.Data)
}

func (g *Generator) Finish() error {
	return nil
}

func (g *Generator) SendMail(sender, receiver, subject, body string) error {
	var c *smtp.Client
	var err error

	startTime := time.Now()
	defer func() {
		g.recordMetric(time.Since(startTime), err)
	}()

	if g.o.DisableConnectionReuse {
		c, err = g.getSmtpClient()
		if err != nil {
			return err
		}
		defer func() {
			c.Quit()
			c.Close()
		}()
	} else {
		c = g.client
	}

	// Set the sender and recipient first
	if err = c.Mail(sender, nil); err != nil {
		return err
	}
	if err = c.Rcpt(receiver); err != nil {
		return err
	}

	// Send the email body.
	var wc io.WriteCloser
	wc, err = c.Data()
	if err != nil {
		return err
	}

	// TODO: add mail headers (to, from, subject etc)
	// TODO: verify the body is not empty
	_, err = io.Copy(wc, strings.NewReader(body))
	if err != nil {
		return err
	}

	err = wc.Close()
	if err != nil {
		return err
	}

	return nil
}

func (g *Generator) recordMetric(d time.Duration, err error) {
	var traceInfo stats.TraceInfo
	traceInfo.Type = stats.SmtpTrace
	traceInfo.Key = g.o.Target
	traceInfo.Subkey = g.o.Target
	if err != nil && !errors.Is(err, context.Canceled) {
		traceInfo.Error = true
	}
	if !traceInfo.Error {
		traceInfo.Total = d
	}

	g.stats.RecordMetric(&traceInfo)
}

func (g *Generator) getSmtpClient() (*smtp.Client, error) {
	c, err := smtp.Dial(g.o.Target)
	if err != nil {
		return nil, err
	}

	if g.o.Username != "" {
		auth := sasl.NewPlainClient("", g.o.Username, g.o.Password)
		if err := c.Auth(auth); err != nil {
			return nil, err
		}
	}

	if !g.o.Plaintext {
		if err := c.StartTLS(g.tlsConfig); err != nil {
			return nil, err
		}
	}

	return c, nil
}
