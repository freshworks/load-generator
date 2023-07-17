package http

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		switch r.RequestURI {
		case "/delayme":
			time.Sleep(50 * time.Millisecond)

		case "/setcookie":
			c := &http.Cookie{Name: "hello", Value: "world", HttpOnly: false}
			http.SetCookie(w, c)

		case "/hello":
			fmt.Fprintf(w, "Hello from server")

		case "/redirectme":
			http.Redirect(w, r, "/hello", http.StatusFound)

		case "/checkheader":
			if r.Header.Get("customheader") != "customvalue" ||
				r.Header.Get("customheaderglobal") != "customvalueglobal" {
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	u, err := url.ParseRequestURI(ts.URL)
	require.Nil(t, err)

	sts := stats.New("id", 1, 1, 0, false)
	sts.Start()

	setup := func(u *url.URL, disableCookieJar ...bool) *Generator {
		o := NewOptions()
		o.Method = "GET"
		o.Url = *u
		o.ProxyHeaders = map[string]string{"my-foo-proxy-header": "bar"}
		if len(disableCookieJar) > 0 {
			o.DisableCookieJar = disableCookieJar[0]
		}
		g := NewGenerator(0, *o, context.Background(), 1, sts)
		require.NotNil(t, g)

		return g
	}

	t.Run("Basic", func(t *testing.T) {
		g := setup(u)

		err = g.Init()
		assert.Nil(t, err)

		err = g.InitDone()
		assert.Nil(t, err)

		err = g.Tick()
		assert.Nil(t, err)

		err = g.Finish()
		assert.Nil(t, err)
	})

	t.Run("CheckMetrics", func(t *testing.T) {
		g := setup(u)
		err = g.Init()
		assert.Nil(t, err)
		err = g.InitDone()
		assert.Nil(t, err)

		resp, err := g.Do("GET", u.String()+"/delayme", nil, "")
		assert.Nil(t, err)

		r := getStatResultFor(sts, u.String(), resp.Request.URL.Path)
		require.NotNil(t, r)
		assert.Equal(t, int64(1), r.Histogram.Count)
	})

	t.Run("DoFormUrl", func(t *testing.T) {
		g := setup(u)

		err = g.Init()
		assert.Nil(t, err)

		params := map[string][]string{
			"hello": []string{"world"},
		}
		_, err := g.DoFormUrl("POST", u.String(), nil, params)
		assert.Nil(t, err)
	})

	t.Run("DiscardResponse", func(t *testing.T) {
		g := setup(u)

		err = g.Init()
		assert.Nil(t, err)

		resp, err := g.Do("GET", u.String()+"/hello", nil, "")
		assert.Nil(t, err)
		if assert.True(t, resp.StatusCode == http.StatusOK) {
			b, err := ioutil.ReadAll(resp.Body)
			require.Nil(t, err)
			assert.Equal(t, "Hello from server", string(b))
		}
	})

	t.Run("DoFormMultipart", func(t *testing.T) {
		g := setup(u)

		err = g.Init()
		assert.Nil(t, err)

		content := []byte("temporary file's content")
		tmpfile, err := ioutil.TempFile("", "example")
		require.Nil(t, err)

		defer os.Remove(tmpfile.Name())

		_, err = tmpfile.Write(content)
		require.Nil(t, err)

		err = tmpfile.Close()
		require.Nil(t, err)

		params := map[string][]string{
			"hello":  []string{"world"},
			"myfile": []string{"@" + tmpfile.Name()},
		}

		_, err = g.DoFormMultipart("POST", u.String(), nil, params)
		assert.Nil(t, err)
	})

	t.Run("GetCookies", func(t *testing.T) {
		g := setup(u)

		err = g.Init()
		assert.Nil(t, err)

		_, err := g.Do("GET", u.String()+"/setcookie", nil, "")
		assert.Nil(t, err)

		c, err := g.GetCookies(u.String())
		assert.Nil(t, err)
		found := false
		for _, cookie := range c {
			if cookie.Name == "hello" && cookie.Value == "world" {
				found = true
			}
		}
		assert.True(t, found, "Cookie missing")
	})

	t.Run("CookieJarDisabled", func(t *testing.T) {
		g := setup(u, true)

		err = g.Init()
		assert.Nil(t, err)

		_, err := g.Do("GET", u.String()+"/setcookie", nil, "")
		assert.Nil(t, err)

		c, err := g.GetCookies(u.String())
		require.Error(t, err)
		require.Nil(t, c)
	})

	t.Run("CheckRedirectNotFollowed", func(t *testing.T) {
		g := setup(u)

		err = g.Init()
		assert.Nil(t, err)

		resp, err := g.Do("GET", u.String()+"/redirectme", nil, "")
		assert.Nil(t, err)
		assert.Equal(t, resp.StatusCode, http.StatusFound)
	})

	t.Run("VerifyCustomHeadersAdded", func(t *testing.T) {
		g := setup(u)
		err = g.Init()
		assert.Nil(t, err)

		g.options.Headers["customheaderglobal"] = "customvalueglobal"
		g.Headers["customheader"] = "customvalue"

		resp, err := g.Do("GET", u.String()+"/checkheader", nil, "")
		assert.Nil(t, err)
		assert.Equal(t, resp.StatusCode, http.StatusOK)
	})

	t.Run("ProxyTestHttp", func(t *testing.T) {
		target := u.String() + "/" + uuid.NewString()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Must be full URL
			require.Equal(t, target, r.URL.String())
			// Proxy header shouldn't be present for http requests
			require.Empty(t, r.Header.Get("my-foo-proxy-header"))
			w.Write([]byte("hello"))
		}))
		defer ts.Close()

		g := setup(u)
		g.client.Transport.(*http.Transport).Proxy = func(r *http.Request) (*url.URL, error) {
			return url.Parse(ts.URL)
		}

		err = g.Init()
		assert.Nil(t, err)

		resp, err := g.Do("GET", target, nil, "")
		require.Nilf(t, err, "%v", err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		b, _ := io.ReadAll(resp.Body)
		require.Equal(t, "hello", string(b))
	})

	t.Run("ProxyTestHttps", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodConnect, r.Method)
			require.Equal(t, "example.com:443", r.Host)
			v := r.Header.Get("my-foo-proxy-header")
			require.Equal(t, "bar", v)
		}))
		defer ts.Close()

		uu, err := url.ParseRequestURI("https://example.com")
		require.Nil(t, err)

		g := setup(uu)
		g.client.Transport.(*http.Transport).Proxy = func(r *http.Request) (*url.URL, error) {
			return url.Parse(ts.URL)
		}

		err = g.Init()
		assert.Nil(t, err)

		_, err = g.Do("GET", uu.String(), nil, "")
		require.Error(t, err) // we will get error, we don't handle tunneling in the handler
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
