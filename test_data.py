#!/usr/bin/env python3
"""
Test script to populate Redis queue with sample data for testing the worker
"""

import json
import redis
from datetime import datetime, timedelta

def create_test_data():
    """Create sample customer records for testing"""
    base_time = datetime.utcnow()
    
    test_records = [
        {
            "customerId": "1",
            "name": "John Doe",
            "email": "john@example.com",
            "createdAt": base_time.isoformat() + "Z"
        },
        {
            "customerId": "2", 
            "name": "Jane Smith",
            "email": "jane@example.com",
            "createdAt": (base_time + timedelta(seconds=5)).isoformat() + "Z"
        },
        {
            "customerId": "3",
            "name": "Bob Johnson", 
            "email": "bob@example.com",
            "createdAt": (base_time + timedelta(seconds=10)).isoformat() + "Z"
        },
        {
            "customerId": "4",
            "name": "Alice Brown",
            "email": "alice@example.com", 
            "createdAt": (base_time + timedelta(seconds=15)).isoformat() + "Z"
        },
        {
            "customerId": "5",
            "name": "Charlie Wilson",
            "email": "charlie@example.com",
            "createdAt": (base_time + timedelta(seconds=20)).isoformat() + "Z"
        },
        # Test rate limiting with same email
        {
            "customerId": "6",
            "name": "John Doe",
            "email": "john@example.com",  # Same email as first record
            "createdAt": (base_time + timedelta(seconds=25)).isoformat() + "Z"
        },
        {
            "customerId": "7",
            "name": "Rachel Smith",
            "email": "rachel@example.com",
            "createdAt": (base_time + timedelta(seconds=30)).isoformat() + "Z"
        }
    ]
    
    # Add more records for the same email to test rate limiting
    for i in range(8, 15):
        test_records.append({
            "customerId": str(i),
            "name": "John Doe",
            "email": "john@example.com",  # This should trigger rate limiting
            "createdAt": (base_time + timedelta(seconds=30 + i)).isoformat() + "Z"
        })
    
    return test_records

def populate_redis_queue(host='localhost', port=6379, queue_name='customer_queue'):
    """Populate Redis queue with test data"""
    try:
        # Connect to Redis
        redis_client = redis.Redis(host=host, port=port, decode_responses=True)
        
        # Test connection
        redis_client.ping()
        print(f"Connected to Redis at {host}:{port}")
        
        # Clear existing queue
        redis_client.delete(queue_name)
        print(f"Cleared existing queue: {queue_name}")
        
        # Add test records
        test_records = create_test_data()
        
        for record in test_records:
            redis_client.rpush(queue_name, json.dumps(record))
            print(f"Added record: {record['customerId']} - {record['email']}")
        
        print(f"\nTotal records added to queue: {len(test_records)}")
        print(f"Queue length: {redis_client.llen(queue_name)}")
        
    except redis.RedisError as e:
        print(f"Redis error: {e}")
    except Exception as e:
        print(f"Error: {e}")

def check_queue_status(host='localhost', port=6379, queue_name='customer_queue'):
    """Check current queue status"""
    try:
        redis_client = redis.Redis(host=host, port=port, decode_responses=True)
        redis_client.ping()
        
        queue_length = redis_client.llen(queue_name)
        print(f"Current queue length: {queue_length}")
        
        if queue_length > 0:
            # Show first few records
            records = redis_client.lrange(queue_name, 0, min(4, queue_length - 1))
            print("\nFirst few records in queue:")
            for i, record_str in enumerate(records):
                try:
                    record = json.loads(record_str)
                    print(f"  {i+1}. {record['customerId']} - {record['email']}")
                except json.JSONDecodeError:
                    print(f"  {i+1}. Invalid JSON: {record_str}")
        
    except redis.RedisError as e:
        print(f"Redis error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "status":
            check_queue_status()
        elif sys.argv[1] == "populate":
            populate_redis_queue()
        else:
            print("Usage: python test_data.py [populate|status]")
    else:
        print("Populating Redis queue with test data...")
        populate_redis_queue()
        print("\nQueue status after population:")
        check_queue_status()
