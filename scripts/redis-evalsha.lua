local redis = require('redis')

-- An example to load Redis Lua script and call it on every tick

local redis_script = [[
local key = KEYS[1]
local change = ARGV[1]

local value = redis.call("GET", key)
if not value then
  value = 0
end

value = value + change
redis.call("SET", key, value)

return value
]]

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

   local a = client:ScriptLoad(ctx, redis_script)
   scriptsha, err = a:Result()
   if err ~= nil then
      Log:Errorf("redis lua script load failed: %v", err)
      return err
   end

   Log:Infof("loaded redis lua script, SHA1 = %v", scriptsha)

   return nil
end

function tick()
   local cmd = client:EvalSha(ctx, scriptsha, {"foo"}, 3)

   if cmd:Err() ~= nil then
      Log:Errorf("redis evalsha failed: %v", cmd:Err())
      return cmd:Err()
   end

   Log:Infof("redis result=%v", cmd:Val())
   return nil
end
