local http = require('http')
local grpc = require('grpc')
local json = require('json')

-- Change this
local grpc_endpoint = os.getenv("EXT_AUTHZ_ENDPOINT") or "localhost:1030"

local print_response = true -- true

-- These may have to be adjusted as per target server configuration
local HOST_NAME_PATTERN = "%d-something.foo.com"
local MAX_CUSTOMERS = 100000
local GRPC_PAYLOAD = [[{"attributes":{"request":{"time":"%s","http":{"method":"GET","path":"/","host":"%s","scheme":"http","protocol":"http1.1"}}}}]]

function init()
   Log:Info("Initializing ... ")

   math.randomseed(os.time())

   o = grpc.Options()
   -- Log:Warnf("=== %+v", o)

   o.Target = grpc_endpoint
   o.Plaintext = true
   -- o.MaxConcurrentStreams = 1000 -- LG.RequestRate
   o.DiscardResponse = not print_response

   grpc_client, err = grpc.New(o)
   if err ~= nil then
      Log:Errorf("gRPC error %v", err)
      return err
   end

   return nil
end

function tick()
   local time = os.date('!%Y-%m-%dT%H:%M:%SZ', os.time())
   local host = HOST_NAME_PATTERN:format(math.random(MAX_CUSTOMERS))
   local p = GRPC_PAYLOAD:format(time, host)

   local msg, err = grpc_client:Do("envoy.service.auth.v3.Authorization.Check", p, nil)
   if err ~= nil then
      Log:Warnf("grpc error: %v", err)
      return
   end

   if print_response then
      msg = json.decode(msg)
      if msg then
         Log:Infof("Response: %v", msg)
      end
   end

   return nil
end
