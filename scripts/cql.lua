local cql = require('cql')

local cql_sess = nil

function init(args)
   local o = cql.Options()
   o.Targets = {"localhost:9042"}
   o.Plaintext = true
   o.Username = "foo"
   o.Password = "1234"
   o.DisablePeersLookup = true
   o.TrackMetricsPerNode = true
   
   local cg, err = cql.New(o)
   if err ~= nil then
      Log:Errorf("%v", err)
      return err
   end

   cql_sess = cg.Session
end

function tick()
   return cql_sess:Query('select uuid() from system.local'):Exec()
end
