local psql = require('psql')

-- 100 SQL calls in one tick and compute metrics for batch of 100 calls as
-- single unit

-- ex:
-- create postgres server:
--    docker run -it --name postgres  -e POSTGRES_PASSWORD=password -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres:12
-- run this script
--    ./lg script --duration 10s --requestrate 1 ./scripts/psql.lua
local db = nil

function init()
   local o = psql.Options()
   o.ConnectionString = "postgresql://postgres@localhost:5432/postgres"
   local psql_client, err = psql.New(o)
   if err ~= nil then
      Log:Errorf("%v", err)
      return err
   end

   db = psql_client.DB
   err = db:Ping()
   if err ~= nil then
      Log:Errorf("%v", err)
      return err
   end
end

function tick()
   return exec_psql_queries()
end

function exec_psql_queries()
   local metric_name = "my_http_req_with_100_queries"
   LG:BeginCustomMetrics(metric_name)
   for num = 1,100 do
      local r, err = db:Query("SELECT 1")
      if err then
         Log:Errorf("Error %v %v\n", err, r)
         LG:EndCustomMetricsWithError(metric_name)
         return
      end

      local err = r:Close()
      if err then
         Log:Errorf("%v", err)
      end
   end

   LG:EndCustomMetrics(metric_name)
end
