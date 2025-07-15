-- Simple MongoDB Lua test script following MySQL pattern
local mongo = require("mongo")

-- Global variables
local mongo_generator = nil
local collection = nil

function init()
    Log:Info("Simple MongoDB Lua Test Started")
    
    -- Create MongoDB client with options
    local options = mongo.Options()
    options.ConnectionString = "mongodb://localhost:27017"
    options.Database = "luatest"
    options.Collection = "testcol"
    options.Username = "admin"
    options.Password = "admin"
    options.AuthDB = "admin"
    options.Operation = "find"
    options.Filter = '{"status":"active"}'
    
    local err
    mongo_generator, err = mongo.New(options)
    if err ~= nil then
        Log:Errorf("MongoDB connection error: %v", err)
        return err
    end
    
    -- Access the MongoDB collection directly (like MySQL script accesses DB)
    collection = mongo_generator.Collection
    
    Log:Info("Connected to MongoDB via Lua - collection accessible")
    return nil
end

function tick()
    Log:Debug("Performing MongoDB operations via Lua")
    
    -- Perform multiple MongoDB operations in one tick
    local metric_name = "lua_mongo_mixed_ops"
    LG:BeginCustomMetrics(metric_name)
    
    -- Perform 3 different operations
    perform_find_operation()
    perform_insert_operation()
    perform_count_operation()
    
    LG:EndCustomMetrics(metric_name)
end

function perform_find_operation()
    -- Use the MongoDB generator's Find method
    local err = mongo_generator:Find('{"status":"active"}')
    if err ~= nil then
        Log:Errorf("MongoDB find error: %v", err)
    end
end

function perform_insert_operation()
    local doc = string.format('{"name":"LuaUser","created_at":"%s","lua_generated":true}', 
                             os.date("%Y-%m-%d %H:%M:%S"))
    local err = mongo_generator:Insert(doc)
    if err ~= nil then
        Log:Errorf("MongoDB insert error: %v", err)
    end
end

function perform_count_operation()
    -- Use aggregate to count documents
    local pipeline = '[{"$match":{"lua_generated":true}},{"$count":"total"}]'
    local err = mongo_generator:Aggregate(pipeline)
    if err ~= nil then
        Log:Errorf("MongoDB aggregate error: %v", err)
    end
end

function finished()
    Log:Info("Simple MongoDB Lua Test Completed")
    if mongo_generator ~= nil then
        local err = mongo_generator:Finish()
        if err ~= nil then
            Log:Errorf("MongoDB finish error: %v", err)
        end
        Log:Info("MongoDB connection closed")
    end
end

Log:Info("Simple MongoDB Lua script loaded")