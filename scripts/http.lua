local http = require('http')
local utils = require("utils")

-- An example to hit some external service

local http_client = nil

function init()
   Log:Info("Initializing ... ")
   Log:Infof("Request rate: %v", LG.RequestRate)
   Log:Infof("Scripts args: %v", LG.ScriptArgs)

   local o = http.Options()
   o.Headers = { ["Hello"] = "World" }

   http_client, err = http.New(o)
   if err ~= nil then
      Log:Errorf("Http error %v", err)
      return err
   end

   return nil
end

function tick()
   Log:Debugf("tick")
   resp, err = http_client:Do("GET",
                       "http://google.com/",nil,"")
   if err then
      if string.match(err:Error(), "context canceled") then
         return
      end
      Log:Errorf("Failed to get content: %v", err)
      return
   end

   Log:Infof("%v", resp.Status)

   _, err = utils.IoUtilReadAll(resp.Body)
   if err then
      Log:Infof("Failed to read resp body: %v", err)
   end

   return
end

function finished()
   print("finished called")
end

Log:Info("Global called")
