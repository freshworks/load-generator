local redis = require('redis')


local client = nil
local ctx = LG:Context()

function init(args)
   local o = redis.Options()
   o.Target = "127.0.0.1:6379"

   local redis, err = redis.New(o)
   if err ~= nil then
      Log:Errorf("%v", err)
      return err
   end

   client = redis.Client

   return nil
end

function tick()
   local cmd = client:IncrBy(ctx, "foo", 2)
   if cmd:Err() ~= nil then
      Log:Errorf("redis increment failed: %v", cmd:Err())
      return cmd:Err()
   end

   Log:Infof("redis result=%+v", cmd:Val())
   return nil
end
