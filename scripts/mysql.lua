local mysql = require('mysql')

-- 100 SQL calls in one tick and compute metrics for batch of 100 calls as
-- single unit

local db = nil

function init()
   local o = mysql.Options()
   o.Target = "root:@/mysql"

   local mysql_client, err = mysql.New(o)
   if err ~= nil then
      Log:Errorf("%v", err)
      return err
   end

   db = mysql_client.DB
end

function tick()
   return exec_sql_queries(queries)
end

function exec_sql_queries(queries)
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

   return
end
