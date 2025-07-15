-- Comprehensive MongoDB Lua test demonstrating all operations
local mongo = require("mongo")

-- Global variables
local mongo_generator = nil
local operation_counter = 0

function init()
    Log:Info("Comprehensive MongoDB Lua Test Started")
    math.randomseed(os.time())
    
    -- Create MongoDB client with options
    local options = mongo.Options()
    options.ConnectionString = "mongodb://localhost:27017"
    options.Database = "comprehensive_test"
    options.Collection = "products"
    options.Username = "admin"
    options.Password = "admin"
    options.AuthDB = "admin"
    
    local err
    mongo_generator, err = mongo.New(options)
    if err ~= nil then
        Log:Errorf("MongoDB connection error: %v", err)
        return err
    end
    
    Log:Info("Connected to MongoDB for comprehensive testing")
    return nil
end

function tick()
    operation_counter = operation_counter + 1
    
    -- Cycle through different operation patterns
    local pattern = operation_counter % 4
    
    if pattern == 0 then
        -- E-commerce product search pattern
        ecommerce_search_pattern()
    elseif pattern == 1 then
        -- Inventory management pattern
        inventory_management_pattern()
    elseif pattern == 2 then
        -- Analytics pattern
        analytics_pattern()
    else
        -- Data maintenance pattern
        maintenance_pattern()
    end
end

function ecommerce_search_pattern()
    local metric_name = "ecommerce_search"
    LG:BeginCustomMetrics(metric_name)
    
    -- Search for products in different categories
    local categories = {"electronics", "clothing", "books", "home"}
    local category = categories[math.random(#categories)]
    
    local search_filter = string.format('{"category":"%s","price":{"$gte":10,"$lte":1000},"in_stock":true}', category)
    
    local err = mongo_generator:Find(search_filter)
    if err ~= nil then
        Log:Errorf("Search error: %v", err)
        LG:EndCustomMetricsWithError(metric_name)
        return
    end
    
    LG:EndCustomMetrics(metric_name)
    Log:Debugf("Performed product search for category: %s", category)
end

function inventory_management_pattern()
    local metric_name = "inventory_management"
    LG:BeginCustomMetrics(metric_name)
    
    -- Add new product
    local product_id = 1000 + math.random(9999)
    local prices = {19.99, 29.99, 49.99, 99.99, 199.99}
    local categories = {"electronics", "clothing", "books", "home"}
    
    local new_product = string.format([[{
        "product_id": %d,
        "name": "Product_%d",
        "category": "%s",
        "price": %.2f,
        "in_stock": true,
        "stock_quantity": %d,
        "created_by": "lua_script",
        "created_at": "%s"
    }]], 
    product_id,
    product_id,
    categories[math.random(#categories)],
    prices[math.random(#prices)],
    math.random(1, 100),
    os.date("%Y-%m-%d %H:%M:%S"))
    
    local err = mongo_generator:Insert(new_product)
    if err ~= nil then
        Log:Errorf("Insert error: %v", err)
        LG:EndCustomMetricsWithError(metric_name)
        return
    end
    
    -- Update stock levels
    local update_filter = '{"in_stock":true,"stock_quantity":{"$lt":10}}'
    local update_doc = '{"$set":{"low_stock_alert":true,"last_checked":"' .. os.date("%Y-%m-%d %H:%M:%S") .. '"}}'
    
    err = mongo_generator:Update(update_filter, update_doc)
    if err ~= nil then
        Log:Errorf("Update error: %v", err)
        LG:EndCustomMetricsWithError(metric_name)
        return
    end
    
    LG:EndCustomMetrics(metric_name)
    Log:Debugf("Added product %d and updated stock alerts", product_id)
end

function analytics_pattern()
    local metric_name = "analytics_queries"
    LG:BeginCustomMetrics(metric_name)
    
    -- Product analytics aggregation
    local analytics_pipeline = [[
    [
        {"$match": {"created_by": "lua_script"}},
        {"$group": {
            "_id": "$category",
            "total_products": {"$sum": 1},
            "avg_price": {"$avg": "$price"},
            "total_stock": {"$sum": "$stock_quantity"}
        }},
        {"$sort": {"total_products": -1}},
        {"$limit": 5}
    ]
    ]]
    
    local err = mongo_generator:Aggregate(analytics_pipeline)
    if err ~= nil then
        Log:Errorf("Analytics error: %v", err)
        LG:EndCustomMetricsWithError(metric_name)
        return
    end
    
    LG:EndCustomMetrics(metric_name)
    Log:Debug("Performed analytics aggregation")
end

function maintenance_pattern()
    local metric_name = "data_maintenance"
    LG:BeginCustomMetrics(metric_name)
    
    -- Clean up test data (delete some old records)
    local cleanup_filter = '{"created_by":"lua_script","stock_quantity":0}'
    
    local err = mongo_generator:Delete(cleanup_filter)
    if err ~= nil then
        Log:Errorf("Delete error: %v", err)
        LG:EndCustomMetricsWithError(metric_name)
        return
    end
    
    LG:EndCustomMetrics(metric_name)
    Log:Debug("Performed data cleanup")
end

function finished()
    Log:Infof("Comprehensive MongoDB Lua Test Completed. Total operations: %d", operation_counter)
    if mongo_generator ~= nil then
        local err = mongo_generator:Finish()
        if err ~= nil then
            Log:Errorf("MongoDB finish error: %v", err)
        end
        Log:Info("MongoDB connection closed")
    end
end

Log:Info("Comprehensive MongoDB Lua script loaded")