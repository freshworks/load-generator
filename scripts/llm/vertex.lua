local http = require('http')
local json = require('json')
local utils = require('utils')

local opts = nil

local http_client = nil
local gcp_base_url = nil
local chat_data_text_bison = nil
local chat_data_chat_bison = nil
local req_cnt = nil

function init(args)
   opts = LG.ScriptArgs

   if not opts.gcp_region then
      Log:Errorf("GCP region option is not provided")
      return {}
   end

   if not opts.gcp_project_id then
      Log:Errorf("GCP project id option is not provided")
      return {}
   end

   if not opts.gcp_api_key then
      Log:Errorf("GCP api key option is not provided")
      return {}
   end

   if not opts.model then
      Log:Errorf("model option is not provided")
      return {}
   end

   gcp_base_url = string.format("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s", opts.gcp_region, opts.gcp_project_id, opts.gcp_region)

   local o = http.Options()
   http_client = http.New(o)

   local file = io.open(LG.ScriptDir .. "/vertex_text_bison.json", "rb")
   if not file then
      Log:Errorf("data file vertex_text_bison.json could not be found")
      return {}
   end

   local content = file:read "*all"
   file:close()
   chat_data_text_bison, err = json.decode(content)
   if err ~= nil then
      Log:Errorf("Failed to parse json file: %v", err)
      return {}
   end

   local file = io.open(LG.ScriptDir .. "/vertex_chat_bison.json", "rb")
   if not file then
      Log:Errorf("data file vertex_chat_bison.json could not be found")
      return {}
   end

   local content = file:read "*all"
   file:close()
   chat_data_chat_bison, err = json.decode(content)
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

   local gcp_url
   local req

   if startswith(opts.model, "text-bison") then
      -- https://us-central1-aiplatform.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/publishers/google/models/${MODEL_ID}:predict -d
      req = {
         ["instances"] = {
            ["content"] = chat_data_text_bison[math.random(1, #chat_data_text_bison)],
         },
         ["parameters"] = {
            ["maxOutputTokens"] = opts.max_tokens,
            ["temperature"] = opts.temperature,
            ["topK"] = opts.top_k,
            ["topP"] = opts.top_p,
         }
      }
      
      gcp_url = gcp_base_url .. "/publishers/google/models/" .. opts.model .. ":predict"
   elseif startswith(opts.model, "chat-bison") then
      req = {
         ["instances"] = {
            chat_data_chat_bison[math.random(1, #chat_data_chat_bison)],
         },
         ["parameters"] = {
            ["maxOutputTokens"] = opts.max_tokens,
            ["temperature"] = opts.temperature,
            ["topK"] = opts.top_k,
            ["topP"] = opts.top_p,
         }
      }
      gcp_url = gcp_base_url .. "/publishers/google/models/" .. opts.model .. ":predict"
   else
      Log:Errorf("Unknown model %v", opts.model)
      return {}
   end

   local headers = {
      ["Authorization"] = "Bearer " .. opts.gcp_api_key,
      ["Content-Type"] = "application/json"
   }

   LG:BeginCustomMetrics(opts.model)
   local resp, err = http_client:Do("POST", gcp_url, headers, json.encode(req))
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
      end
      return nil
   end
   LG:EndCustomMetrics(opts.model)

   local b = get_resp_body(resp)
   if b ~= nil then
      local resp_json = json.decode(b)
      Log:Infof("Response: %v", resp_json)
      -- LG:RecordRawMetrics("total_tokens", resp_json.usage.total_tokens)
      -- LG:RecordRawMetrics("input_tokens", resp_json.usage.prompt_tokens)
      -- LG:RecordRawMetrics("output_tokens", resp_json.usage.completion_tokens)
      -- Log:Infof("%v\n", resp_json)
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
   local parser = argparse("--", "script to do gcp LLM benchmark", usage_examples)

   parser:option("--gcp-region", "GCP Region")
   parser:option("--gcp-project-id", "GCP project id")
   parser:option("--gcp-api-key", "GCP API key")
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
