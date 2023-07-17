local grpc = require('grpc')

--[=====[
This assumes there is a working helloworld grpc server.
Instructions to run one:

   git clone https://github.com/grpc/grpc-go.git
   cd grpc-go/examples/helloworld/greeter_server/
   go run main.go
--]=====]

local json = require('json')

local grpc_client = nil

function init()
   Log:Info("Initializing ... ")
   o = grpc.Options()

   -- We can also use gRPC reflection to discover the service.
   -- Here we are using a proto file to discover the service as an example.
   o.Proto = {LG.ScriptDir .. "/helloworld.proto"}
   o.ImportPath = {LG.ScriptDir}
   o.Target = "127.0.0.1:50051"
   o.Plaintext = true
   -- o.MaxConcurrentStreams = LG.RequestRate
   -- o.DiscardResponse = true

   grpc_client, err = grpc.New(o)
   if err ~= nil then
      Log:Errorf("gRPC error %v", err)
   end

   return err
end

function tick()
   Log:Debugf("tick")
   local msg, err = grpc_client:Do("helloworld.Greeter.SayHello", [[{"name": "example"}]], nil)
   if err ~= nil then
      Log:Warnf("grpc error: %v", err)
   else
      msg = json.decode(msg)
      if msg then
         Log:Infof("Response: %v", msg.message)
      end
   end

   return
end

function finished()
   print("finished called")
end
