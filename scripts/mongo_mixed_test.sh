#!/bin/bash

# MongoDB Mixed Operations Load Test
# This script runs different MongoDB operations to simulate a realistic workload

echo "=== MongoDB Mixed Operations Load Test ==="
echo "Starting comprehensive MongoDB load testing..."

# MongoDB connection details
MONGO_URI="mongodb://admin:admin@localhost:27017"
DATABASE="ecommerce"

echo ""
echo "1. Creating sample products (Insert operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection products --operation insert \
    --document '{"name":"Laptop","price":999.99,"category":"electronics","stock":50,"brand":"TechCorp"}' \
    --rate 8 --duration 8s --warmup 2s

echo ""
echo "2. Product catalog searches (Find operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection products --operation find \
    --filter '{"category":"electronics","price":{"$lte":1500}}' \
    --rate 15 --duration 8s --warmup 2s

echo ""
echo "3. Creating user accounts (Insert operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection users --operation insert \
    --document '{"name":"John Doe","email":"john@example.com","age":30,"status":"active","preferences":{"newsletter":true}}' \
    --rate 5 --duration 8s --warmup 2s

echo ""
echo "4. User profile lookups (Find operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection users --operation find \
    --filter '{"status":"active","age":{"$gte":18,"$lte":65}}' \
    --rate 12 --duration 8s --warmup 2s

echo ""
echo "5. Updating user activity (Update operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection users --operation update \
    --filter '{"status":"active"}' \
    --update '{"$set":{"last_login":"2024-01-15","activity_score":{"$inc":1}}}' \
    --rate 6 --duration 8s --warmup 2s

echo ""
echo "6. Inventory updates (Update operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection products --operation update \
    --filter '{"category":"electronics"}' \
    --update '{"$inc":{"views":1},"$set":{"last_viewed":"2024-01-15"}}' \
    --rate 4 --duration 8s --warmup 2s

echo ""
echo "7. Analytics aggregation (Aggregate operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection users --operation aggregate \
    --filter '[{"$match":{"status":"active"}},{"$group":{"_id":"$age","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":10}]' \
    --rate 2 --duration 8s --warmup 2s

echo ""
echo "8. Product analytics (Aggregate operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection products --operation aggregate \
    --filter '[{"$group":{"_id":"$category","total_products":{"$sum":1},"avg_price":{"$avg":"$price"},"total_stock":{"$sum":"$stock"}}}]' \
    --rate 3 --duration 8s --warmup 2s

echo ""
echo "9. Cleanup old records (Delete operations)..."
./lg mongo --connection $MONGO_URI --database $DATABASE --collection users --operation delete \
    --filter '{"status":"inactive","last_login":{"$lt":"2023-01-01"}}' \
    --rate 1 --duration 6s --warmup 2s

echo ""
echo "=== MongoDB Mixed Operations Load Test Completed ==="
echo "This test simulated a realistic e-commerce workload with:"
echo "- Product catalog management"
echo "- User account operations" 
echo "- Real-time updates"
echo "- Analytics queries"
echo "- Data cleanup operations"