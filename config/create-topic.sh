#!/bin/bash

# Create a properties file for authentication
cat > /tmp/client.properties << EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
EOF

# Create the test topic
docker compose exec kafka-scram kafka-topics.sh --create \
  --topic scram-test-topic \
  --bootstrap-server localhost:9093 \
  --command-config /tmp/client.properties \
  --partitions 1 \
  --replication-factor 1

# Clean up
rm /tmp/client.properties
