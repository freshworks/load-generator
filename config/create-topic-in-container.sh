#!/bin/bash

# Create a script to run inside the container
cat > /tmp/create-topic.sh << 'EOF'
#!/bin/bash

# Create a properties file for authentication
cat > /tmp/client.properties << EOT
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
EOT

# Create the test topic
kafka-topics.sh --create \
  --topic scram-test-topic \
  --bootstrap-server localhost:9093 \
  --command-config /tmp/client.properties \
  --partitions 1 \
  --replication-factor 1

# Clean up
rm /tmp/client.properties
EOF

# Copy the script to the container
docker compose cp /tmp/create-topic.sh kafka-scram:/tmp/create-topic.sh

# Make it executable and run it
docker compose exec kafka-scram chmod +x /tmp/create-topic.sh
docker compose exec kafka-scram /tmp/create-topic.sh

# Clean up
rm /tmp/create-topic.sh
