#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka-SCRAM to be ready..."
sleep 10

# Create SCRAM users
echo "Creating SCRAM users..."
docker exec kafka-scram kafka-configs --bootstrap-server localhost:9093 \
  --alter --add-config 'SCRAM-SHA-256=[password=user-secret],SCRAM-SHA-512=[password=user-secret]' \
  --entity-type users --entity-name user

docker exec kafka-scram kafka-configs --bootstrap-server localhost:9093 \
  --alter --add-config 'SCRAM-SHA-256=[password=admin-secret],SCRAM-SHA-512=[password=admin-secret]' \
  --entity-type users --entity-name admin

echo "SCRAM users created successfully!"

# Create a test topic
echo "Creating test topic..."
docker exec kafka-scram kafka-topics --bootstrap-server localhost:9093 \
  --create --topic scram-test-topic --partitions 1 --replication-factor 1 \
  --command-config /etc/kafka/client.properties

echo "Setup completed!"
