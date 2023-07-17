local http = require('http')
local json = require('json')
local utils = require('utils')

local opts = nil

local http_client = nil
local azure_base_url = nil
local model = nil
local max_tokens = 1024
local temperature = 0.9
local top_p = 1
local num_completions = 1
local chat_data_gpt = nil
local chat_data_davinci = nil
local req_cnt = nil

function init(args)
   opts = LG.ScriptArgs

   if not opts.azure_endpoint then
      Log:Errorf("Azure endpoint option is not provided")
      return {}
   end

   if not opts.azure_deployment then
      Log:Errorf("Azure deployment option is not provided")
      return {}
   end

   if not opts.azure_api_key then
      Log:Errorf("Azure api key option is not provided")
      return {}
   end

   azure_base_url = opts.azure_endpoint .. "/openai/deployments/" .. opts.azure_deployment

   local o = http.Options()
   o.StreamResponse = opts.stream_response
   http_client = http.New(o)

   local file = io.open(LG.ScriptDir .. "/openai_gptX.json", "rb")
   if not file then
      Log:Errorf("data file openai_gptX.json could not be found")
      return {}
   end

   local content = file:read "*all"
   file:close()
   chat_data_gpt, err = json.decode(content)
   if err ~= nil then
      Log:Errorf("Failed to parse json file: %v", err)
      return {}
   end

   local file = io.open(LG.ScriptDir .. "/openai_davinci.json", "rb")
   if not file then
      Log:Errorf("data file openai_davinci.json could not be found")
      return {}
   end

   local content = file:read "*all"
   file:close()
   chat_data_davinci, err = json.decode(content)
   if err ~= nil then
      Log:Errorf("Failed to parse json file: %v", err)
      return {}
   end

   req_cnt = utils.GetCounter("request_counter")

   return nil
end

function tick()
   if opts.num_requests > 0 and req_cnt:Increment() > opts.num_requests then
      Log:Infof("Finished %v requests", opts.num_requests)
      return {}
   end

   local req
   local azure_url
   
   if startswith(opts.model, "gpt") then
      req = {
         ["model"] = opts.model,
         ["messages"] = chat_data_gpt[math.random(1, #chat_data_gpt)],
         ["max_tokens"] = opts.max_tokens,
         ["temperature"] = opts.temperature,
         ["top_p"] = opts.top_p,
         ["n"] = opts.num_completions,
         ["stream"] = opts.stream_response,
      }
      azure_url = azure_base_url .. "/chat/completions?api-version=2023-03-15-preview"
   elseif startswith(opts.model, "text-davinci") then
      req = {
         ["model"] = opts.model,
         ["prompt"] = chat_data_davinci[math.random(1, #chat_data_davinci)],
         ["max_tokens"] = opts.max_tokens,
         ["temperature"] = opts.temperature,
         ["top_p"] = opts.top_p,
         ["n"] = opts.num_completions,
         ["stream"] = opts.stream_response,
      }
      azure_url = azure_base_url .. "/completions?api-version=2022-12-01"
   else
      Log:Errorf("Unknown model %v", opts.model)
      return {}
   end

   local headers = {
      ["Api-Key"] = opts.azure_api_key,
      ["Content-Type"] = "application/json; charset=utf-8"
   }

   if opts.stream_response then
      LG:BeginCustomMetrics(opts.model .. "#time-to-first-token")
   end

   LG:BeginCustomMetrics(opts.model)
   local resp, err = http_client:Do("POST", azure_url, headers, json.encode(req))
   if err ~= nil then
      Log:Errorf("Error: %v", err)
      LG:EndCustomMetricsWithError(opts.model)
      req_cnt:Decrement()
      return nil
   end

   if resp.StatusCode < 200 or resp.StatusCode >= 300 then
      LG:EndCustomMetricsWithError(opts.model)
      Log:Errorf("Error: code=%v body=%v", resp.StatusCode, get_resp_body(resp))
      if resp.StatusCode == 429 then
         LG:RecordRawMetrics("rate_limited", 1)
         req_cnt:Decrement()
         utils.Sleep(1000)
      end
      return nil
   end

   if opts.stream_response then
      local first_token_metric_reported = false
      local r = utils.BufferedReader(resp.Body)
      local err
      repeat
         line, err = r:ReadBytes(string.byte("\n"))
         if err ~= nil then
            if err:Error() ~= "EOF" then
               Log:Errorf("Error: %v", err)
            end
            break
         end
         Log:Infof("Response: %v", utils.ByteToString(line))
         if not first_token_metric_reported then
            first_token_metric_reported = true
            LG:EndCustomMetrics(opts.model .. "#time-to-first-token")
         end
      until err ~= nil
      LG:EndCustomMetrics(opts.model)
      resp.Body:Close()
   else
      LG:EndCustomMetrics(opts.model)

      local b = get_resp_body(resp)
      if b ~= nil then
         local resp_json = json.decode(b)
         Log:Infof("Response: %v", resp_json.usage)
         LG:RecordRawMetrics("total_tokens", resp_json.usage.total_tokens)
         LG:RecordRawMetrics("input_tokens", resp_json.usage.prompt_tokens)
         LG:RecordRawMetrics("output_tokens", resp_json.usage.completion_tokens)
         Log:Infof("%v\n", resp_json)
      end
   end

   return nil
end

function get_resp_body(resp)
   if resp.Body == nil then
      return nil
   end

   b, err = utils.IoUtilReadAll(resp.Body)
   if err then
      Log.Warnf("Failed to read response body %v", err)
      return nil
   end

   return utils.ByteToString(b)
end

function args(cmdline)
   local argparse = require("argparse")
   local parser = argparse("--", "script to do azure openai benchmark", usage_examples)

   parser:option("--azure-deployment", "Azure deployment name")
   parser:option("--azure-endpoint", "Azure endpoint URL")
   parser:option("--azure-api-key", "Azure API key")
   parser:option("--model", "Model to use", "gpt-3.5-turbo")
   parser:option("--max-tokens", "Max tokens", 1024):convert(tonumber)
   parser:option("--temperature", "Temperature", 0.9)
   parser:option("--top-p", "Top p", 1)
   parser:option("--num-completions", "Number of completions", 1)
   parser:option("--num-requests", "Number of requests to make", 0):convert(tonumber)
   parser:flag("--stream-response", "Stream the response", false)

   return parser:parse(cmdline)
end

function startswith(text, prefix)
    return text:find(prefix, 1, true) == 1
end
