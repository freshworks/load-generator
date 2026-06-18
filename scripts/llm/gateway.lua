-- gateway.lua - generic OpenAI-compatible chat-completions load driver.
--
-- This script has no built-in Bifrost/LiteLLM branching. Point it at any
-- OpenAI-compatible gateway URL, pass the model that gateway expects, and it
-- will POST chat-completion requests.
--
-- Gateway URL rules:
--   * If --gateway-url already ends in /chat/completions, it is used as-is.
--   * Otherwise --path is appended. The default path is /chat/completions.
--
-- Examples:
--   # Bifrost local setup. Bifrost expects the /v1 prefix and provider/model.
--   go run . script --duration 10s --requestrate 5 scripts/llm/gateway.lua -- \
--     --gateway-url http://127.0.0.1:8080/v1 \
--     --model openai/gpt-4o-mini
--
--   # LiteLLM local setup. LiteLLM expects the model alias and bearer key.
--   go run . script --duration 10s --requestrate 5 scripts/llm/gateway.lua -- \
--     --gateway-url http://127.0.0.1:4000 \
--     --model gpt-4o-mini \
--     --api-key sk-local-litellm
--
--   # Load a random prompt object from JSON on each tick.
--   go run . script --duration 10s --requestrate 5 scripts/llm/gateway.lua -- \
--     --gateway-url http://127.0.0.1:4000 \
--     --model gpt-4o-mini \
--     --api-key sk-local-litellm \
--     --prompts-file scripts/llm/gateway_prompts.json

local http = require("http")
local json = require("json")
local utils = require("utils")

local opts
local http_client
local req_cnt
local prompt_entries

local default_path = "/chat/completions"
local default_model = "gpt-4o-mini"

local prompts = {
    "What is the capital of France?",
    "Write a short haiku about load testing.",
    "Explain retry backoff in one sentence.",
    "Return the word Hello.",
    "What is one benefit of API gateways?",
}

local function load_prompts_file(path)
    local file = io.open(path, "rb")
    if not file then
        Log:Errorf("prompts file %s could not be opened", path)
        return nil
    end

    local content = file:read("*all")
    file:close()

    local decoded, err = json.decode(content)
    if err ~= nil then
        Log:Errorf("failed to parse prompts file %s: %v", path, err)
        return nil
    end
    if type(decoded) ~= "table" or #decoded == 0 then
        Log:Errorf("prompts file %s must contain a non-empty JSON array", path)
        return nil
    end

    for i, entry in ipairs(decoded) do
        if type(entry) ~= "table" then
            Log:Errorf("prompts file entry %d must be a JSON object", i)
            return nil
        end
        if entry.messages == nil and entry.prompt == nil then
            Log:Errorf("prompts file entry %d must contain messages or prompt", i)
            return nil
        end
    end

    return decoded
end

local function trim_trailing_slash(value)
    if value == nil then return "" end
    return tostring(value):gsub("/+$", "")
end

local function join_url(base, path)
    base = trim_trailing_slash(base)
    if path == nil or path == "" then return base end
    if path:sub(1, 1) ~= "/" then path = "/" .. path end
    return base .. path
end

local function request_url()
    local url = trim_trailing_slash(opts.gateway_url)
    if url:match("/chat/completions$") then
        return url
    end
    return join_url(url, opts.path)
end

local function metric_prefix()
    if opts.metric_name ~= nil and opts.metric_name ~= "" then
        return opts.metric_name
    end
    return "llm-gateway"
end

local function parse_extra_headers(headers)
    if opts.headers == nil or opts.headers == "" then return end
    for pair in opts.headers:gmatch("[^,]+") do
        local key, value = pair:match("^%s*(.-)%s*:%s*(.-)%s*$")
        if key and key ~= "" then
            headers[key] = value
        end
    end
end

local function read_body(resp)
    if resp == nil or resp.Body == nil then return nil end
    local b, err = utils.IoUtilReadAll(resp.Body)
    resp.Body:Close()
    if err then
        Log:Warnf("failed to read response body: %v", err)
        return nil
    end
    return utils.ByteToString(b)
end

local function token_usage(resp_json)
    local usage = resp_json and resp_json.usage or nil
    if usage == nil then
        return { total = 0, prompt = 0, completion = 0 }
    end

    local prompt = usage.prompt_tokens or usage.input_tokens or usage.promptTokens or usage.inputTokens or 0
    local completion = usage.completion_tokens or usage.output_tokens or usage.completionTokens or usage.outputTokens or 0
    local total = usage.total_tokens or usage.totalTokens or usage.total or 0
    if total == 0 then total = prompt + completion end

    return { total = total, prompt = prompt, completion = completion }
end

local function response_content(resp_json)
    if resp_json == nil or resp_json.choices == nil or resp_json.choices[1] == nil then
        return nil
    end
    local message = resp_json.choices[1].message
    if message == nil then return nil end
    return message.content
end

local function build_body()
    local messages

    if opts.prompt ~= nil and opts.prompt ~= "" then
        messages = {
            { role = "user", content = opts.prompt },
        }
    elseif prompt_entries ~= nil and #prompt_entries > 0 then
        local entry = prompt_entries[math.random(1, #prompt_entries)]
        if entry.messages ~= nil then
            messages = entry.messages
        else
            messages = {
                { role = "user", content = entry.prompt },
            }
        end
    else
        messages = {
            { role = "user", content = prompts[math.random(1, #prompts)] },
        }
    end

    return json.encode({
        model = opts.model,
        messages = messages,
        max_tokens = opts.max_tokens,
        temperature = opts.temperature,
        stream = false,
    })
end

local function send_request()
    local prefix = metric_prefix()
    local headers = {
        ["Content-Type"] = "application/json; charset=utf-8",
    }

    if opts.api_key ~= nil and opts.api_key ~= "" then
        headers["Authorization"] = "Bearer " .. opts.api_key
    end
    parse_extra_headers(headers)

    LG:BeginCustomMetrics(prefix .. ":chat")
    local resp, err = http_client:Do("POST", request_url(), headers, build_body())
    if err ~= nil then
        LG:EndCustomMetricsWithError(prefix .. ":chat")
        LG:RecordRawMetrics(prefix .. ":transport_error", 1)
        if opts.debug then Log:Errorf("transport error: %v", err) end
        return
    end

    local status = resp.StatusCode or 0
    LG:RecordRawMetrics(prefix .. ":status:" .. tostring(status), 1)
    local body = read_body(resp)

    if status < 200 or status >= 300 then
        LG:EndCustomMetricsWithError(prefix .. ":chat")
        if opts.debug then
            Log:Errorf("status=%.0f body=%s", status, body or "")
        end
        return
    end

    LG:EndCustomMetrics(prefix .. ":chat")

    if body == nil or body == "" then
        return
    end

    local resp_json, err = json.decode(body)
    if err ~= nil then
        if opts.debug then Log:Warnf("gateway returned non-json body: %s", body) end
        return
    end

    local usage = token_usage(resp_json)
    LG:RecordRawMetrics(prefix .. ":total_tokens", usage.total)
    LG:RecordRawMetrics(prefix .. ":prompt_tokens", usage.prompt)
    LG:RecordRawMetrics(prefix .. ":completion_tokens", usage.completion)

    if opts.debug then
        Log:Infof("status=%.0f url=%s model=%s content=%s total_tokens=%.0f",
            status, request_url(), opts.model, response_content(resp_json) or "", usage.total)
    end
end

function init()
    opts = LG.ScriptArgs

    if opts.gateway_url == nil or opts.gateway_url == "" then
        Log:Errorf("--gateway-url is required")
        return {}
    end
    if opts.path == nil or opts.path == "" then opts.path = default_path end
    if opts.model == nil or opts.model == "" then opts.model = default_model end

    if opts.prompts_file ~= nil and opts.prompts_file ~= "" then
        prompt_entries = load_prompts_file(opts.prompts_file)
        if prompt_entries == nil then return {} end
        Log:Infof("loaded %.0f prompt entries from %s", #prompt_entries, opts.prompts_file)
    end

    local o = http.Options()
    o.PrintCurl = opts.print_curl or false
    o.StreamResponse = false
    http_client = http.New(o)
    req_cnt = utils.GetCounter("generic_gateway_request_counter")

    Log:Infof("generic gateway target url=%s model=%s", request_url(), opts.model)
    return nil
end

function tick()
    if opts.num_requests and opts.num_requests > 0 and req_cnt:Increment() > opts.num_requests then
        Log:Infof("Finished %v requests", opts.num_requests)
        return {}
    end

    send_request()
    return nil
end

function args(cmdline)
    local argparse = require("argparse")
    local parser = argparse("--", "Generic OpenAI-compatible chat-completions benchmark")

    parser:option("--gateway-url", "Gateway base URL or full /chat/completions URL")
    parser:option("--path", "Path appended when gateway-url is not a full chat-completions URL", default_path)
    parser:option("--model", "Model string to send exactly as provided", default_model)
    parser:option("--api-key", "Optional bearer token", "")
    parser:option("--metric-name", "Metric prefix", "llm-gateway")
    parser:option("--prompt", "Fixed user prompt. Empty means random prompt", "")
    parser:option("--prompts-file", "JSON array of prompt objects. Each entry needs messages or prompt", "")
    parser:option("--max-tokens", "Max tokens", 16):convert(tonumber)
    parser:option("--temperature", "Temperature", 0):convert(tonumber)
    parser:option("--num-requests", "Number of requests per worker. 0 = unlimited", 0):convert(tonumber)
    parser:option("--headers", "Extra request headers, comma-separated 'k:v,k2:v2'", "")
    parser:flag("--print-curl", "Print equivalent curl for each request", false)
    parser:flag("--debug", "Log response status, URL, content, and usage", false)

    return parser:parse(cmdline)
end
