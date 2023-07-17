package lua

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/emersion/go-smtp"
	"github.com/freshworks/load-generator/internal/cql"
	"github.com/freshworks/load-generator/internal/psql"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/freshworks/load-generator/internal/utils"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
)

func TestScriptGenerator(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	//logrus.SetLevel(logrus.DebugLevel)

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
	require.Nil(err)

	sts := stats.New("id", 1, 1, 0, false)
	sts.Start()
	defer sts.Stop()

	setup := func(script string, args []string) (
		/* load generator object */ *Generator /* sink where log data is written into */, *bytes.Buffer, error) {
		sink := new(bytes.Buffer)

		log := logrus.WithFields(logrus.Fields{"Id": script})
		logrus.SetFormatter(&plainFormatter{})

		log.Logger.Out = sink
		log.Logger.Hooks.Add(&logHook{t: t})

		options := NewOptions()
		options.Script = script
		options.Args = args

		lg := NewGenerator(*options, 1, 1, 1, context.Background(), sts)
		require.NotNil(lg)
		lg.log = log

		luaCompiled = nil

		err = lg.Init()
		if err != nil {
			return nil, sink, err
		}

		err = lg.InitDone()
		if err != nil {
			return nil, sink, err
		}

		return lg, sink, nil
	}

	t.Run("Basic", func(t *testing.T) {
		script := `
			Log:Infof("====global called====")

	           function args(cmdline)
	              Log:Infof("====args called==== %v", cmdline)
                      if cmdline ~= nil then
                         Log:Infof("====cmdline==== %v", cmdline)
                      end
                      return {}
	           end

	           function init()
	              Log:Infof("====init called====")
	           end

	           function tick()
	              Log:Infof("====tick called====")
	           end

	           function finish()
	              Log:Infof("====finish called====")
	           end`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, sink, err := setup(f, nil)
		require.Nil(err)

		err = g.Tick()
		assert.Nil(err)

		err = g.Finish()
		assert.Nil(err)

		require.Contains(sink.String(), "====global called====", "global statement was not executed")
		assert.Contains(sink.String(), "====init called====", "init function was not called")
		assert.Contains(sink.String(), "====args called====", "arg function was not called")
		assert.Contains(sink.String(), "====cmdline====", "arg function was not passed cmdline argument")
		assert.Contains(sink.String(), "====tick called====", "tick function was not called")
		assert.Contains(sink.String(), "====finish called====", "finish function was not called")
	})

	t.Run("Script Args", func(t *testing.T) {
		script := `
	           function args(cmdline)
	              Log:Infof("====args called====")
                      local argparse = require('argparse')
                      local parser = argparse("--", "my script description")
                      parser:option("--hello", "my option argument", "")
                      parser:flag("--disable-foo", "my flag argument", false)
                      return parser:parse(cmdline)
	           end

	           function init()
	              Log:Infof("====init called====")
                      assert(LG.ScriptArgs.hello == "world")
                      assert(LG.ScriptArgs.disable_foo == true)
	           end

	           function tick()
	              Log:Infof("====tick called====")
	           end

	           function finish()
	              Log:Infof("====finish called====")
	           end`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, sink, err := setup(f, []string{"--hello", "world", "--disable-foo"})
		require.Nil(err)

		require.Contains(sink.String(), "====args called====", "arg function was not called")
		require.Contains(sink.String(), "====init called====", "init function was not called")

		err = g.Tick()
		require.Nil(err)
		require.Contains(sink.String(), "====tick called====", "tick function was not called")

		err = g.Finish()
		require.Nil(err)
		require.Contains(sink.String(), "====finish called====", "finish function was not called")
	})

	t.Run("Script Args Readonly", func(t *testing.T) {
		script := `
	           function args(cmdline)
                      local argparse = require('argparse')
                      local parser = argparse("--", "my script description")
                      parser:flag("--disable-foo", "my flag argument", false)
                      return parser:parse(cmdline)
	           end

	           function init()
	              Log:Infof("====init called====")
                      assert(LG.ScriptArgs.disable_foo == true)
                      local opts = LG.ScriptArgs
                      -- try to modify opts, we should get readonly error
                      opts.Foo = "hello"
	           end

	           function tick()
                   end
`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		_, sink, err := setup(f, []string{"--disable-foo"})
		require.NotNilf(err, "script output: %v", sink.String())
		require.Contains(err.Error(), "modifying readonly table")
	})

	t.Run("Script Args Global Mutation Error", func(t *testing.T) {
		script := `
	           function args(cmdline)
                      -- Try to set a global variable, we should get an error
                      foo = "bar"
                      local argparse = require('argparse')
                      local parser = argparse("--", "my script description")
                      parser:flag("--disable-foo", "my flag argument", false)
                      return parser:parse(cmdline)
	           end

	           function init()
	              Log:Infof("====init called====")
                      assert(foo == nil, "global variable foo should be nil")
	           end

	           function tick()
                   end
`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		_, _, err = setup(f, []string{"--disable-foo"})
		require.NotNilf(err, "expected not nil but got %v", err)
		require.Contains(err.Error(), "modifying readonly table")
	})

	t.Run("InitFailure", func(t *testing.T) {
		script := `
	           function init()
                      return {}
	           end

	           function tick()
	              return true
	           end

	           function finish()
	           end`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		_, _, err = setup(f, nil)
		require.NotNil(err)
	})

	t.Run("TickFailure", func(t *testing.T) {
		script := `
	           function init()
                      return
	           end

	           function tick()
	              return {}
	           end

	           function finish()
	              Log:Infof("====finish called====")
	           end`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Tick()
		assert.NotNil(err)
	})

	t.Run("API/LG", func(t *testing.T) {

		script := `
                   function init()
                      assert(LG ~= nil, "LG is nil")
                      assert(LG.Id ~= nil, "LG.Id is nil")
	              assert(LG.Concurrency ~= nil, "LG.Concurrency is nil")
	              assert(LG.Map ~= nil, "LG.Map is nil")
	              assert(LG.RequestRate ~= nil, "LG.RequestRate is nil")
	              assert(LG.ScriptDir ~= nil, "LG.ScriptDir is nil")

                      assert(type(LG.ShouldQuit) == "function", "LG.ShouldQuit")
                      assert(LG:ShouldQuit() == false, "LG:ShouldQuit")

                      assert(type(LG.SetTickDataFile) == "function", "LG.SetTickDataFile")
                      assert(LG:SetTickDataFile("something") == nil, "LG:SetTickDataFile")
                   end

                   function tick()
                   end
`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("API/LG/Http", func(t *testing.T) {

		script := `
                           local http = require('http')
                           local utils = require('utils')
                           local http_client = nil

                           assert(http.Options ~= nil, "http.Options missing")
                           assert(http.New ~= nil, "http.New missing")

	                   function init()
			      Log:Info("Initializing ... ")
			      local o = http.Options()
			      http_client, err = http.New(o)
			      if err ~= nil then
				 Log:Infof("http error %v", err)
                                 return err
			      end

                              assert(http_client ~= nil, "http missing")
                              assert(http_client.Headers ~= nil, "http.Headers missing")
                              assert(http_client.Do ~= nil, "http.Do missing")
                              assert(http_client.DoFormUrl ~= nil, "http.DoFormUrl missing")
                              assert(http_client.DoFormMultipart ~= nil, "http.DoFormMultipart missing")
			      return nil
			   end

			   function tick()
                              local resp, err = http_client:Do("GET", "{{.Target}}" .. "/hello", nil, "")
                              assert(err == nil, "Error making request")
                              assert(resp.Body ~= nil, "response body is nil")
                              local b, err = utils.IoUtilReadAll(resp.Body)
                              assert(err == nil, "error reading response body")
			      assert(utils.ByteToString(b) == "Hello from server", utils.ByteToString(b))
			   end
`

		var s bytes.Buffer
		tl, err := template.New("").Parse(script)
		require.Nil(err)
		err = tl.Execute(&s, struct{ Target string }{u.String()})
		require.Nilf(err, "failed: %v", err)

		f, err := utils.GetTempFile("scriptest", s.Bytes())
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Tick()
		assert.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("API/LG/MySQL", func(t *testing.T) {

		script := `
                   local mysql = require('mysql')

                   function init()
                      assert(mysql.Options ~= nil, "mysql.Options is nil")
                      assert(mysql.New ~= nil, "mysql.New is nil")

                      local o = mysql.Options()
                      local m = mysql.New(o)
                      assert(m ~= nil)
                   end

                   function tick()
                   end
`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("API/LG/Redis", func(t *testing.T) {

		script := `
                   local redis = require('redis')

                   function init()
                      assert(redis.Options ~= nil, "redis.Options is nil")
                      assert(redis.New ~= nil, "redis.New is nil")

                      local o = redis.Options()
                      local r = redis.New(o)
                      assert(r ~= nil)
                   end

                   function tick()
                   end
`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("API/LG/GRPC", func(t *testing.T) {

		svr, port := setupGRPCSvr(t)
		defer svr.Stop()

		script := `
                           local grpc = require('grpc')
                           local utils = require('utils')
                           local json = require('json')

                           assert(grpc.Options ~= nil, "grpc.Options missing")
                           assert(grpc.New ~= nil, "grpc.New missing")

                           local grpc_client = nil

	                   function init()
			      Log:Info("Initializing ... ")
			      o = grpc.Options()
			      o.Proto = {"../grpc/testing/test.proto"}
			      o.Target = "127.0.0.1:{{.GRPCPort}}"
			      o.Plaintext = true
                              o.Deadline = 100000000

			      grpc_client, err = grpc.New(o)
			      if err ~= nil then
				 Log:Infof("gRPC error %v", err)
			      end

			      return err
			   end

			   function tick()
			      Log:Infof("Tick")
                              local msg, err = grpc_client:Do("grpc.testing.TestService/UnaryCall", [[{"payload":{"body":"aGVsbG93b3JsZA=="}}]], nil)
                              if err ~= nil then
                                 Log:Errorf("grpc error: %v", err)
                                 return err
                              end

                              msg = json.decode(msg)
                              msg = utils.base64:DecodeString(msg.payload.body)
                              Log:Infof("Response: %v", utils.ByteToString(msg))
			   end
			   `

		var s bytes.Buffer
		tl, err := template.New("").Parse(script)
		require.Nil(err)
		err = tl.Execute(&s, struct{ GRPCPort int }{port})
		require.Nil(err)

		f, err := utils.GetTempFile("scriptest", s.Bytes())
		require.Nil(err)
		defer os.Remove(f)

		g, sink, err := setup(f, nil)
		require.Nil(err, sink.String())

		sink.Reset()
		err = g.Tick()
		assert.Contains(sink.String(), "Response: helloworld", sink.String())
		assert.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("API/LG/Cql", func(t *testing.T) {

		pool, resource, err := startCassandra()
		require.NoError(err)
		port := resource.GetPort("9042/tcp")

		defer func() {
			g := cql.NewGenerator(1, cql.GeneratorOptions{Targets: []string{"127.0.0.1:" + port}}, context.Background(), 1, sts)
			if g.Session != nil {
				g.Session.Close()
			}
			if err = pool.Purge(resource); err != nil {
				log.Fatalf("Could not purge resource: %s", err)
			}
		}()

		script := `
			   local cql = require('cql')

			   local cql_sess = nil

			   function init(args)
			      local o = cql.Options()
			      o.Targets = {"127.0.0.1:{{.CqlPort}}"}
			      o.Plaintext = true
			      o.Username = "cassandra"
			      o.Password = "cassandra"

			      local cg, err = cql.New(o)
			      if err ~= nil then
				 Log:Errorf("%v", err)
				 return err
			      end

			      cql_sess = cg.Session
			   end

			   function tick()
			      local res = cql_sess:Query('select uuid() from system.local'):Exec()
			      if res == nil then
				  Log:Info("cql query success")
			      else
				  Log:Infof("cql query failed: %v", res)
			      end
			   end
			   `

		var s bytes.Buffer
		tl, err := template.New("").Parse(script)
		require.Nil(err)
		err = tl.Execute(&s, struct{ CqlPort string }{port})
		require.Nil(err)

		f, err := utils.GetTempFile("scriptest", s.Bytes())
		require.Nil(err)
		defer os.Remove(f)

		g, sink, err := setup(f, nil)
		require.Nil(err, sink.String())

		sink.Reset()
		err = g.Tick()
		assert.Contains(sink.String(), "cql query success", sink.String())
		assert.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("API/LG/Psql", func(t *testing.T) {

		var connectionString string
		var cleanupTestResources func()

		testcert := &psql.Cert{}
		testcert.PopulateSelfSigned("test", "test.com")
		connectionString, cleanupTestResources, err = psql.RunPostgresDB("15", testcert)
		defer cleanupTestResources()
		require.Nil(err)

		scriptSegment1 := fmt.Sprintf(`
					local psql = require('psql')
	
					local psqldb = nil
	
					function init()
							local o = psql.Options()
							o.ConnectionString = "%v"`, connectionString)
		scriptSegment2 := `o.Query = "SELECT 1"
							local psql_client, err = psql.New(o)
							if err ~= nil then
								Log:Errorf("%v", err)
								return err
							end
	
							psqldb = psql_client.DB
					end
	
					function tick()
							local metric_name = "DB_TEST_QUERY"
							LG:BeginCustomMetrics(metric_name)
	
							local r, err = psqldb:Query("SELECT 1")
							if err then
								Log:Errorf("Error %v %v\n", err, r)
								LG:EndCustomMetricsWithError(metric_name)
								return
							else
								Log:Info("psql query success")
							end
	
							local err = r:Close()
							if err then
								Log:Errorf("%v", err)
							end
	
							LG:EndCustomMetrics(metric_name)
					end
					`
		script := fmt.Sprintf("%v%v", scriptSegment1, scriptSegment2)

		f, err := utils.GetTempFile("scriptest", []byte(script))

		require.Nil(err)
		defer os.Remove(f)

		g, sink, err := setup(f, nil)

		require.Nil(err, sink.String())
		sink.Reset()
		err = g.Tick()

		assert.Contains(sink.String(), "psql query success", sink.String())
		assert.Nil(err)

		err = g.Finish()
		assert.Nil(err)

	})

	t.Run("API/LG/SMTP", func(t *testing.T) {

		username := uuid.NewString()
		password := uuid.NewString()
		svr, err := setupSmtpSvr(username, password)
		require.NoError(err)
		defer svr.Close()

		script := `
                           local smtp = require('smtp')

                           assert(smtp.Options ~= nil, "smtp.Options missing")
                           assert(smtp.New ~= nil, "smtp.New missing")

                           local smtp_client = nil

	                   function init()
			      Log:Info("Initializing ... ")
			      o = smtp.Options()
			      o.Target = "{{.Target}}"
			      o.Plaintext = true
                              o.Username = "{{.Username}}"
			      o.Password = "{{.Password}}"

			      smtp_client, err = smtp.New(o)
			      if err ~= nil then
				 Log:Infof("smtp error %v", err)
			      end

			      return err
			   end

			   function tick()
			      Log:Infof("Tick")
                              local err = smtp_client:SendMail("sender@example.com", "receiver@example.com", "hello sub", "how are you")
                              if err ~= nil then
                                 Log:Errorf("smtp error: %v", err)
                                 return err
                              end
                              Log:Info("sent mail")
			   end
			   `

		var s bytes.Buffer
		tl, err := template.New("").Parse(script)
		require.Nil(err)
		err = tl.Execute(&s, struct {
			Target   string
			Username string
			Password string
		}{svr.Addr, username, password})
		require.Nil(err)

		f, err := utils.GetTempFile("scriptest", s.Bytes())
		require.Nil(err)
		defer os.Remove(f)

		g, sink, err := setup(f, nil)
		require.Nil(err, sink.String())

		sink.Reset()
		err = g.Tick()
		assert.Contains(sink.String(), "sent mail", sink.String())
		assert.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("UtilsModule", func(t *testing.T) {

		script := `
                   local utils = require('utils')

                   assert(type(utils.GenerateRandomWords) == "function", "utils.GenerateRandomWords")
                   assert(utils.GenerateRandomWords(5) ~= "", "utils.GenerateRandomWords")

                   assert(type(utils.GenerateRandomText) == "function", "utils.GenerateRandomText")
                   assert(utils.GenerateRandomText(5, 1) ~= "", "utils.GenerateRandomText")

                   assert(type(utils.base64) == "userdata", "utils.base64")
                   assert(type(utils.base64.DecodeString) == "function", "utils.base64.DecodeString")
                   assert(type(utils.base64.EncodeToString) == "function", "utils.base64.EncodeToString")

                   assert(utils.base64:EncodeToString("hello world") == "aGVsbG8gd29ybGQ=", "utils.base64:EncodeToString encoding failed")
                   local res, err = utils.base64:DecodeString("aGVsbG8gd29ybGQ=")
                   assert(err == nil, "utils.base64:DecodeString failed")
                   local msg = utils.ByteToString(res)
                   assert(msg == "hello world", "decoded message is not \"hello world\"")

                   assert(utils.IoUtilReadAll ~= nil, "utils.IoUtilReadAll is missing")
                   assert(utils.ByteToString ~= nil, "utils.ByteToString is missing")

                   assert(type(utils.Sleep) == "function", "utils.Sleep is missing")
                   utils.Sleep(1)

                   assert(type(utils.NewUUID) == "function", "utils.NewUUID is missing")
                   local uuid = utils.NewUUID()
                   assert(uuid ~= "", "uuid generation failed " .. uuid)

                   assert(type(utils.GetCounter) == "function", "utils.GetCounter is missing")
                   local foo_counter = utils.GetCounter("foo")
                   assert(foo_counter ~= nil, "Utils.GetCounter failed")
                   assert(-foo_counter == 0, "counter is not initialized to zero")
                   assert(foo_counter:Increment() == 1, "counter:Increment is not 1")

                   local foo_counter2 = utils.GetCounter("foo")
                   assert(-foo_counter2 == 1, "counter is not shared")
                   assert(foo_counter2:Decrement() == 0, "counter:Decrement is not 0")
                   assert(-foo_counter == 0, "foo_counter is not 0")
                   assert(-foo_counter2 == 0, "foo_counter2 is not 0")

                   function tick() end
`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("SqlUtilsModule", func(t *testing.T) {

		script := `
                   local sqlutils = require('sqlutils')

                   assert(type(sqlutils.NewTemplateRepository) == "function", "sqlutils.NewTemplateRepository")
                   assert(type(sqlutils.GetRow) == "function", "sqlutils.GetRow")
                   assert(type(sqlutils.RowsToJson) == "function", "sqlutils.RowsToJson")

                   function tick() end
`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("API/LG/CustomMetrics", func(t *testing.T) {

		script := `
                   function init()
                      assert(LG.BeginCustomMetrics ~= nil)
                      assert(LG.EndCustomMetrics ~= nil)
                      assert(LG.EndCustomMetricsWithError ~= nil)
                      assert(LG.AbortCustomMetrics ~= nil)
                   end

                   function tick()
                      assert(LG:BeginCustomMetrics("hello") == nil)
                      assert(LG:EndCustomMetrics("hello") == nil)

                      LG:BeginCustomMetrics("foo")
                      assert(LG:EndCustomMetricsWithError("foo") == nil)

                      assert(LG:EndCustomMetrics("bar") ~= nil)

                      LG:BeginCustomMetrics("bar")
                      LG:AbortCustomMetrics("bar")
                   end
`

		f, err := utils.GetTempFile("scriptest", []byte(script))
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Tick()
		assert.Nil(err)

		err = g.Finish()
		assert.Nil(err)

		r := getStatResultFor(sts, "custom", "hello")
		require.NotNil(r)
		assert.Equal(int64(1), r.Histogram.Count)

		r = getStatResultFor(sts, "custom", "foo")
		require.NotNil(r)
		// error cases should not increment counter
		assert.Equal(int64(0), r.Histogram.Count)
	})

	t.Run("API/LG/CSV", func(t *testing.T) {

		data := `
mydata1,myval1
mydata2,myval2
mydata3,myval3
`
		d, err := utils.GetTempFile("scriptdata*.csv", []byte(data))
		require.Nil(err)
		defer os.Remove(d)

		script := `
                   csv = require("csv")
                   assert(csv ~= nil)

                   function init()
                      local c, err = csv.Open("{{.DataFile}}")
                      assert(err == nil)

                      local d, err = c:Read()
                      assert(err == nil)
                      assert(d[1] == "mydata1" and d[2] == "myval1")

                      local d, err = c:Read()
                      assert(err == nil)
                      assert(d[1] == "mydata2" and d[2] == "myval2")

                      local d, err = c:Read()
                      assert(err == nil)
                      assert(d[1] == "mydata3" and d[2] == "myval3")

                      local d, err = c:Read()
                      assert(err ~= nil)
                   end

                   function tick()
                   end
`

		var s bytes.Buffer
		tl, err := template.New("").Parse(script)
		require.Nil(err)
		err = tl.Execute(&s, struct{ DataFile string }{d})
		require.Nil(err)

		f, err := utils.GetTempFile("scriptest", s.Bytes())
		require.Nil(err)
		defer os.Remove(f)

		g, _, err := setup(f, nil)
		require.Nil(err)

		err = g.Finish()
		assert.Nil(err)
	})

	t.Run("CheckTickDataCSV", func(t *testing.T) {

		data := `
	mydata1,myval1
	mydata2,myval2
	mydata3,myval3
	`
		d, err := utils.GetTempFile("scriptdata*.csv", []byte(data))
		require.Nil(err)
		defer os.Remove(d)

		script := `
		                   function init()
				      LG:SetTickDataFile("{{.DataFile}}")
				   end

				   function tick(r)
				      Log:Infof("%v", r)
				   end
				   `

		var s bytes.Buffer
		tl, err := template.New("").Parse(script)
		require.Nil(err)
		err = tl.Execute(&s, struct{ DataFile string }{d})
		require.Nil(err)

		f, err := utils.GetTempFile("scriptest", s.Bytes())
		require.Nil(err)
		defer os.Remove(f)

		g, sink, err := setup(f, nil)
		require.Nil(err)

		// Hack
		g.LG.initTickData()
		defer g.LG.finishTickData()

		sink.Reset()
		err = g.Tick()
		require.NoError(err)
		assert.Contains(sink.String(), "mydata1 myval1")

		sink.Reset()
		err = g.Tick()
		require.NoError(err)
		assert.Contains(sink.String(), "mydata2 myval2")

		sink.Reset()
		err = g.Tick()
		require.NoError(err)
		assert.Contains(sink.String(), "mydata3 myval")

		sink.Reset()
		err = g.Tick()
		require.NoError(err)
		assert.Contains(sink.String(), "mydata1 myval1")
	})

	t.Run("CheckTickData", func(t *testing.T) {

		data := `mydata1
	mydata2
	mydata3
	`
		d, err := utils.GetTempFile("scriptdata*", []byte(data))
		require.Nil(err)
		defer os.Remove(d)

		script := `
		                   function init()
				      LG:SetTickDataFile("{{.DataFile}}")
				      return
				   end

				   function tick(r)
				      Log:Infof("%v", r)
				   end
				   `

		var s bytes.Buffer
		tl, err := template.New("").Parse(script)
		require.Nil(err)
		err = tl.Execute(&s, struct{ DataFile string }{d})
		require.Nil(err)

		f, err := utils.GetTempFile("scriptest", s.Bytes())
		require.Nil(err)
		defer os.Remove(f)

		g, sink, err := setup(f, nil)
		require.Nil(err)

		// Hack
		g.LG.initTickData()
		defer g.LG.finishTickData()

		sink.Reset()
		err = g.Tick()
		require.NoError(err)
		assert.Contains(sink.String(), "mydata1")

		sink.Reset()
		err = g.Tick()
		require.NoError(err)
		assert.Contains(sink.String(), "mydata2")

		sink.Reset()
		err = g.Tick()
		require.NoError(err)
		assert.Contains(sink.String(), "mydata3")

		sink.Reset()
		err = g.Tick()
		require.NoError(err)
		assert.Contains(sink.String(), "mydata1")
	})
}

type plainFormatter struct{}

func (f *plainFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	if entry.Message != "" {
		b.WriteString(entry.Message)
	}

	b.WriteString("\n")
	return b.Bytes(), nil
}

type logHook struct {
	t *testing.T
}

func (l *logHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

func (l *logHook) Fire(e *logrus.Entry) error {
	l.t.Log(e.Message)
	return nil
}

func setupGRPCSvr(t *testing.T) (*grpc.Server, int) {
	svr := grpc.NewServer()
	grpc_testing.RegisterTestServiceServer(svr, testServer{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.Nil(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	go svr.Serve(l)

	return svr, port
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

func startCassandra() (*dockertest.Pool, *dockertest.Resource, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, err
	}
	pool.MaxWait = 60 * time.Second

	err = pool.Client.Ping()
	if err != nil {
		return nil, nil, err
	}

	o := &dockertest.RunOptions{
		Repository: "scylladb/scylla",
		Tag:        "5.1",
		Cmd: []string{
			// Copied from https://github.com/scylladb/scylladb/blob/master/test/cql-pytest/run.py#L197
			"--developer-mode", "1",
			"--ring-delay-ms", "0",
			"--collectd", "0",
			"--smp", "1",
			"--max-networking-io-control-blocks", "100",
			"--unsafe-bypass-fsync", "1",
			"--kernel-page-cache", "1",
			"--flush-schema-tables-after-modification", "false",
			"--auto-snapshot", "0",
			"--skip-wait-for-gossip-to-settle", "0",
			"--logger-log-level", "compaction=warn",
			"--logger-log-level", "migration_manager=warn",
			"--range-request-timeout-in-ms", "300000",
			"--read-request-timeout-in-ms", "300000",
			"--counter-write-request-timeout-in-ms", "300000",
			"--cas-contention-timeout-in-ms", "300000",
			"--truncate-request-timeout-in-ms", "300000",
			"--write-request-timeout-in-ms", "300000",
			"--request-timeout-in-ms", "300000",
			"--experimental-features=udf",
			"--experimental-features=keyspace-storage-options",
			"--enable-user-defined-functions", "1",
			"--authenticator", "PasswordAuthenticator",
			"--authorizer", "CassandraAuthorizer",
			"--strict-allow-filtering", "true",
			"--permissions-update-interval-in-ms", "100",
			"--permissions-validity-in-ms", "100",
		},
	}

	resource, err := pool.RunWithOptions(o)
	if err != nil {
		return nil, nil, err
	}

	retURL := fmt.Sprintf("127.0.0.1:%s", resource.GetPort("9042/tcp"))
	port, _ := strconv.Atoi(resource.GetPort("9042/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		clusterConfig := gocql.NewCluster(retURL)
		clusterConfig.Authenticator = gocql.PasswordAuthenticator{
			Username: "cassandra",
			Password: "cassandra",
		}
		clusterConfig.ProtoVersion = 4
		clusterConfig.Port = port

		session, err := clusterConfig.CreateSession()
		if err != nil {
			return fmt.Errorf("error creating session: %s", err)
		}
		defer session.Close()
		return nil
	}); err != nil {
		return nil, nil, err
	}

	return pool, resource, nil
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
	return nil
}

func (s *Session) Rcpt(to string) error {
	// log.Println("Rcpt to:", to)
	return nil
}

func (s *Session) Data(r io.Reader) error {
	if b, err := ioutil.ReadAll(r); err != nil {
		return err
	} else {
		_ = b
		//log.Println("Data:", string(b))
	}
	return nil
}

func (s *Session) Reset() {}

func (s *Session) Logout() error {
	// log.Println("logout")
	return nil
}
