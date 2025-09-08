#!/bin/sh

# Redis initialization script
# This script initializes the customer_queue in Redis

echo "Initializing Redis with customer_queue..."

# Wait for Redis to be fully ready
sleep 2

# Connect to Redis and initialize the queue
redis-cli << EOF
LPUSH customer_queue "init"
LPOP customer_queue
SET redis_initialized "true"
EXPIRE redis_initialized 86400

LLEN customer_queue
EOF

echo "Redis initialization completed. customer_queue is ready."
