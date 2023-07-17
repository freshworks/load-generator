package smtp

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-smtp"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/freshworks/load-generator/internal/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSMTP(t *testing.T) {
	require := require.New(t)

	username := uuid.NewString()
	password := uuid.NewString()
	svr, err := setupSmtpSvr(username, password)
	require.NoError(err)
	defer svr.Close()

	// fmt.Printf("listening on: %v\n", svr.Addr)

	sts := stats.New("id", 1, 1, 0, false)
	sts.Start()
	defer sts.Stop()

	t.Run("Basic", func(t *testing.T) {
		o := NewOptions()
		o.Plaintext = true
		o.Username = username
		o.Password = password
		o.Target = svr.Addr
		o.From = "foo@example.com"
		o.To = "bar@example.com"
		o.Subject = "hello"
		o.Data = "bye"

		g := NewGenerator(0, *o, context.Background(), 1, sts)
		require.NotNil(g)

		sts.Reset()

		err := g.Init()
		require.Nil(err)

		err = g.InitDone()
		require.Nil(err)

		err = g.Tick()
		require.Nil(err)
		r := getStatResultFor(sts, o.Target, o.Target)
		require.NotNil(t, r)
		require.Equal(int64(1), r.Histogram.Count)

		err = g.Tick()
		require.Nil(err)
		r = getStatResultFor(sts, o.Target, o.Target)
		require.NotNil(t, r)
		require.Equal(int64(2), r.Histogram.Count)

		err = g.Finish()
		require.Nil(err)
	})

	t.Run("With TLS", func(t *testing.T) {
		rootcaDir := t.TempDir()
		f, err := os.CreateTemp(rootcaDir, "root-ca-*")
		require.NoError(err)
		if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: svr.TLSConfig.Certificates[0].Certificate[0]}); err != nil {
			log.Fatalf("Failed to write data to cert.pem: %v", err)
		}
		rootCA := f.Name()
		f.Close()

		o := NewOptions()
		o.Plaintext = false
		o.Username = username
		o.Password = password
		o.Target = fmt.Sprintf(svr.Addr)
		o.From = "foo@example.com"
		o.To = "bar@example.com"
		o.Subject = "hello"
		o.Data = "bye"
		o.TlsServerName = "test"
		o.RootCAs = []string{rootCA}

		g := NewGenerator(0, *o, context.Background(), 1, sts)
		require.NotNil(g)

		sts.Reset()

		err = g.Init()
		require.Nil(err)

		err = g.InitDone()
		require.Nil(err)

		err = g.Tick()
		require.Nil(err)
		r := getStatResultFor(sts, o.Target, o.Target)
		require.NotNil(t, r)
		require.Equal(int64(1), r.Histogram.Count)

		err = g.Tick()
		require.Nil(err)
		r = getStatResultFor(sts, o.Target, o.Target)
		require.NotNil(t, r)
		require.Equal(int64(2), r.Histogram.Count)

		err = g.Finish()
		require.Nil(err)
	})

	t.Run("With TLS Accept Unknown CA", func(t *testing.T) {
		o := NewOptions()
		o.Plaintext = false
		o.Username = username
		o.Password = password
		o.Target = fmt.Sprintf(svr.Addr)
		o.From = "foo@example.com"
		o.To = "bar@example.com"
		o.Subject = "hello"
		o.Data = "bye"
		o.Insecure = true

		g := NewGenerator(0, *o, context.Background(), 1, sts)
		require.NotNil(g)

		sts.Reset()

		err = g.Init()
		require.Nil(err)

		err = g.InitDone()
		require.Nil(err)

		err = g.Tick()
		require.Nil(err)
		r := getStatResultFor(sts, o.Target, o.Target)
		require.NotNil(t, r)
		require.Equal(int64(1), r.Histogram.Count)

		err = g.Tick()
		require.Nil(err)
		r = getStatResultFor(sts, o.Target, o.Target)
		require.NotNil(t, r)
		require.Equal(int64(2), r.Histogram.Count)

		err = g.Finish()
		require.Nil(err)
	})

	t.Run("Wrong Auth", func(t *testing.T) {
		o := NewOptions()
		o.Plaintext = true
		o.Username = username
		o.Password = "barrr"
		o.Target = fmt.Sprintf(svr.Addr)
		o.From = "foo@example.com"
		o.To = "bar@example.com"
		o.Subject = "hello"
		o.Data = "bye"

		g := NewGenerator(0, *o, context.Background(), 1, sts)
		require.NotNil(g)

		sts.Reset()

		err := g.Init()
		require.Error(err)
		require.Contains(err.Error(), "Invalid username or password")
	})

	t.Run("Error", func(t *testing.T) {
		o := NewOptions()
		o.Plaintext = true
		o.Username = username
		o.Password = password
		o.Target = svr.Addr
		o.From = "foo@example.com"
		o.To = "bar@example.com"
		o.Subject = "hello"
		o.Data = "GENERATE ERROR"

		g := NewGenerator(0, *o, context.Background(), 1, sts)
		require.NotNil(g)

		sts.Reset()

		err := g.Init()
		require.Nil(err)

		err = g.InitDone()
		require.Nil(err)

		err = g.Tick()
		require.Error(err)
		r := getStatResultFor(sts, o.Target, o.Target)
		require.NotNil(t, r)
		require.Equal(1, *r.Errors)

		err = g.Tick()
		require.Error(err)
		r = getStatResultFor(sts, o.Target, o.Target)
		require.NotNil(t, r)
		require.Equal(2, *r.Errors)

		err = g.Finish()
		require.Nil(err)
	})
}

func setupSmtpSvr(username, password string) (*smtp.Server, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}

	s := smtp.NewServer(&Backend{username: username, password: password})

	s.Addr = fmt.Sprintf(":%d", l.Addr().(*net.TCPAddr).Port)
	s.Domain = "test"
	s.ReadTimeout = 10 * time.Second
	s.WriteTimeout = 10 * time.Second
	s.MaxMessageBytes = 1024 * 1024
	s.MaxRecipients = 50
	s.AllowInsecureAuth = true

	cert, key, err := utils.GenCert("test org", "test")
	if err != nil {
		return nil, err
	}
	cer, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	s.TLSConfig = &tls.Config{
		Certificates: []tls.Certificate{cer},
	}

	// log.Println("Starting server at", s.Addr)
	go func() {
		if err := s.Serve(l); err != nil {
			log.Fatal(err)
		}
	}()

	return s, nil
}

type Backend struct {
	username string
	password string
}

func (bkd *Backend) NewSession(_ *smtp.Conn) (smtp.Session, error) {
	return &Session{username: bkd.username, password: bkd.password}, nil
}

// A Session is returned after EHLO.
type Session struct {
	username string
	password string
}

func (s *Session) AuthPlain(username, password string) error {
	if username != s.username || password != s.password {
		return errors.New("Invalid username or password")
	}
	return nil
}

func (s *Session) Mail(from string, opts *smtp.MailOptions) error {
	// log.Println("Mail from:", from)
	if strings.Contains(from, "GENERATE_ERROR") {
		return fmt.Errorf("error processing from")
	}
	return nil
}

func (s *Session) Rcpt(to string) error {
	// log.Println("Rcpt to:", to)
	if strings.Contains(to, "GENERATE_ERROR") {
		return fmt.Errorf("error processing rcpt")
	}

	return nil
}

func (s *Session) Data(r io.Reader) error {
	if b, err := ioutil.ReadAll(r); err != nil {
		return err
	} else {
		// log.Println("Data:", string(b))
		if strings.Contains(string(b), "GENERATE ERROR") {
			return fmt.Errorf("error processing email")
		}
	}
	return nil
}

func (s *Session) Reset() {}

func (s *Session) Logout() error {
	// log.Println("logout")
	return nil
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
