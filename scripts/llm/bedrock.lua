local http = require('http')
local json = require('json')
local utils = require('utils')

local opts = nil

local http_client = nil
local aws_base_url = nil
local chat_data_text = nil
local req_cnt = nil

function init(args)
   opts = LG.ScriptArgs

   if not opts.aws_region then
      Log:Errorf("AWS region option is not provided")
      return {}
   end

   if not opts.model then
      Log:Errorf("model option is not provided")
      return {}
   end

   aws_base_url = string.format("https://bedrock.%s.amazonaws.com", opts.aws_region)

   local o = http.Options()
   o.AwsSign = true
   o.AwsSignInfo.Region = opts.aws_region
   o.AwsSignInfo.Service = "bedrock"
   http_client = http.New(o)

   local file = io.open(LG.ScriptDir .. "/bedrock_text.json", "rb")
   if not file then
      Log:Errorf("data file bedrock_text.json could not be found")
      return {}
   end

   local content = file:read "*all"
   file:close()
   chat_data_text, err = json.decode(content)
   if err ~= nil then
      Log:Errorf("Failed to parse json file: %v", err)
      return {}
   end

   local file = io.open(LG.ScriptDir .. "/bedrock_claude.json", "rb")
   if not file then
      Log:Errorf("data file bedrock_claude.json could not be found")
      return {}
   end

   local content = file:read "*all"
   file:close()
   chat_data_claude, err = json.decode(content)
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

   local aws_url
   local req

   if startswith(opts.model, "amazon.titan") then
      req = {
         ["inputText"] = chat_data_text[math.random(1, #chat_data_text)],
         ["textGenerationConfig"] = {
            ["maxTokenCount"] = opts.max_tokens,
            ["temperature"] = opts.temperature,
            ["topP"] = opts.top_p,
         }
      }

      aws_url = aws_base_url .. "/model/" .. opts.model .. "/invoke"
   elseif startswith(opts.model, "anthropic.claude") then
      req = {
         ["prompt"] = chat_data_claude[math.random(1, #chat_data_claude)],
         ["max_tokens_to_sample"] = opts.max_tokens,
         ["temperature"] = opts.temperature,
         ["top_p"] = opts.top_p,
      }

      aws_url = aws_base_url .. "/model/" .. opts.model .. "/invoke"
   else
      Log:Errorf("Unknown model %v", opts.model)
      return {}
   end

   local headers = {
      ["Content-Type"] = "application/json"
   }

   LG:BeginCustomMetrics(opts.model)
   local resp, err = http_client:Do("POST", aws_url, headers, json.encode(req))
   if err ~= nil then
      Log:Errorf("Error: %v", err)
      LG:EndCustomMetricsWithError(opts.model)
      return nil
   end

   if resp.StatusCode < 200 or resp.StatusCode >= 300 then
      Log:Errorf("Error: %v", get_resp_body(resp))
      if resp.StatusCode == 429 then
         LG:RecordRawMetrics("rate_limited", 1)
         req_cnt:Decrement()
         utils.Sleep(1000)
      end
      return nil
   end
   LG:EndCustomMetrics(opts.model)

   local b = get_resp_body(resp)
   if b ~= nil then
      local resp_json = json.decode(b)
      Log:Infof("Response: %v", resp_json)

      if resp_json.inputTextTokenCount ~= nil then
         LG:RecordRawMetrics("total_tokens", resp_json.inputTextTokenCount + resp_json.results[1].tokenCount)
         LG:RecordRawMetrics("input_tokens", resp_json.inputTextTokenCount)
         LG:RecordRawMetrics("output_tokens", resp_json.results[1].tokenCount)
         -- Log:Infof("%v\n", resp_json)
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
   local parser = argparse("--", "script to do aws LLM benchmark", usage_examples)

   parser:option("--aws-region", "AWS Region")
   parser:option("--model", "Model to use")
   parser:option("--max-tokens", "Max tokens", 1024):convert(tonumber)
   parser:option("--temperature", "Temperature", 0.9)
   parser:option("--top-p", "Top p", 1):convert(tonumber)
   parser:option("--top-k", "Top k", 40):convert(tonumber)
   parser:option("--num-requests", "Number of requests to make", 0):convert(tonumber)

   return parser:parse(cmdline)
end

function startswith(text, prefix)
    return text:find(prefix, 1, true) == 1
end
