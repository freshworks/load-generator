-- Run local dockers if needed for testing
--
-- Mysql:
--     docker run --rm --publish 3306:3306 --env MYSQL_ALLOW_EMPTY_PASSWORD=true mysql
-- Redis:
--     docker run --rm --publish 6379:6379 redis

local http = require('http')
local redis = require('redis')
local mysql = require('mysql')

-- An example to hit some external service

local http_client = nil
local redis_client = nil
local mysql_client = nil

local http_endpoint = os.getenv("HTTP_SERVER") or "http://google.com"
local redis_endpoint = os.getenv("REDIS_SERVER") or "localhost:6379"
local mysql_endpoint = os.getenv("MYSQL_SERVER") or "root@tcp(localhost:3306)/"

function init()
   local o = http.Options()
   o.Headers = { ["Hello"] = "World" }
   o.DiscardResponse = true
   http_client, err = http.New(o)
   if err ~= nil then
      Log:Errorf("http error %v", err)
      return err
   end

   local o = redis.Options()
   o.Target = redis_endpoint
   local redis, err = redis.New(o)
   if err ~= nil then
      Log:Errorf("redis %v", err)
      return err
   end
   redis_client = redis.Client

   local o = mysql.Options()
   o.Target = mysql_endpoint
   local mysql, err = mysql.New(o)
   if err ~= nil then
      Log:Errorf("mysql %v", err)
      return err
   end
   mysql_client = mysql.DB

   return nil
end

function tick()
   LG:BeginCustomMetrics("foo_custom_metric")

   -- http call
   resp, err = http_client:Do("GET",
                       "http://google.com/",nil,"")
   if err then
      Log:Errorf("http error: %v", err)
   end

   -- redis call
   local cmd = redis_client:IncrBy(LG:Context(), "foo", 2)
   if cmd:Err() ~= nil then
      Log:Errorf("redis increment failed: %v", cmd:Err())
   end

   -- mysql call
   local r, err = mysql_client:Query("SELECT 1")
   if err then
      Log:Errorf("mysql error %v\n", err)
   end
   r:Close()

   LG:EndCustomMetrics("foo_custom_metric")

   return
end
