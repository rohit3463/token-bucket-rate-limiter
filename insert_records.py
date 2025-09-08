#!/usr/bin/env python3
"""
Insert Records Script

This script takes sample data from test_data.py and inserts it into the Redis queue
named 'customer_queue'. It provides flexible options for inserting different
types of test data scenarios.
himanshu.gupta@gohighlevel.com
"""

import json
import redis
import sys
import time
from datetime import datetime, timedelta
from test_data import create_test_data

class RedisRecordInserter:
    def __init__(self, host='localhost', port=6379, queue_name='customer_queue'):
        """Initialize Redis connection"""
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.redis_client = None
        
    def connect(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.Redis(
                host=self.host, 
                port=self.port, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Test connection
            self.redis_client.ping()
            print(f"✓ Connected to Redis at {self.host}:{self.port}")
            return True
            
        except redis.ConnectionError as e:
            print(f"✗ Failed to connect to Redis: {e}")
            print(f"  Make sure Redis is running on {self.host}:{self.port}")
            return False
        except Exception as e:
            print(f"✗ Unexpected error connecting to Redis: {e}")
            return False
    
    def get_queue_status(self):
        """Get current queue status"""
        try:
            if not self.redis_client:
                if not self.connect():
                    return None
                    
            queue_length = self.redis_client.llen(self.queue_name)
            return {
                'length': queue_length,
                'queue_name': self.queue_name
            }
        except Exception as e:
            print(f"Error getting queue status: {e}")
            return None
    
    def clear_queue(self):
        """Clear the Redis queue"""
        try:
            if not self.redis_client:
                if not self.connect():
                    return False
                    
            self.redis_client.delete(self.queue_name)
            print(f"✓ Cleared queue: {self.queue_name}")
            return True
            
        except Exception as e:
            print(f"✗ Error clearing queue: {e}")
            return False
    
    def insert_records(self, records, clear_first=True):
        """Insert records into Redis queue"""
        try:
            if not self.redis_client:
                if not self.connect():
                    return False
            
            # Clear queue if requested
            if clear_first:
                self.clear_queue()
            
            # Insert records
            inserted_count = 0
            for i, record in enumerate(records):
                try:
                    # Convert record to JSON string
                    record_json = json.dumps(record)
                    
                    # Push to queue (RPUSH adds to the end)
                    self.redis_client.rpush(self.queue_name, record_json)
                    
                    print(f"  {i+1:2d}. Inserted: {record['customerId']} - {record['name']} ({record['email']})")
                    inserted_count += 1
                    
                except (json.JSONEncodeError, KeyError) as e:
                    print(f"  ✗ Skipped invalid record {i+1}: {e}")
                    continue
            
            # Show final status
            final_length = self.redis_client.llen(self.queue_name)
            print(f"\n✓ Successfully inserted {inserted_count} records")
            print(f"✓ Current queue length: {final_length}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error inserting records: {e}")
            return False
    
    def insert_batch_with_delay(self, records, batch_size=5, delay_seconds=2):
        """Insert records in batches with delays (useful for testing rate limiting)"""
        try:
            if not self.redis_client:
                if not self.connect():
                    return False
            
            print(f"Inserting {len(records)} records in batches of {batch_size}")
            print(f"Delay between batches: {delay_seconds} seconds")
            
            total_inserted = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                print(f"\n--- Batch {(i//batch_size) + 1} ---")
                for j, record in enumerate(batch):
                    record_json = json.dumps(record)
                    self.redis_client.rpush(self.queue_name, record_json)
                    print(f"  Inserted: {record['customerId']} - {record['email']}")
                    total_inserted += 1
                
                # Delay before next batch (except for the last batch)
                if i + batch_size < len(records):
                    print(f"Waiting {delay_seconds} seconds before next batch...")
                    time.sleep(delay_seconds)
            
            final_length = self.redis_client.llen(self.queue_name)
            print(f"\n✓ Successfully inserted {total_inserted} records in batches")
            print(f"✓ Current queue length: {final_length}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error in batch insert: {e}")
            return False
    
    def insert_from_json_file(self, file_path, clear_first=True):
        """Insert records from a JSON file"""
        try:
            # Read the JSON file
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # Handle different JSON structures
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and 'records' in data:
                records = data['records']
            else:
                print(f"✗ Invalid JSON structure. Expected a list or object with 'records' key")
                return False
            
            print(f"Loaded {len(records)} records from {file_path}")
            
            # Use existing insert_records method
            return self.insert_records(records, clear_first)
            
        except FileNotFoundError:
            print(f"✗ File not found: {file_path}")
            return False
        except json.JSONDecodeError as e:
            print(f"✗ Invalid JSON in file {file_path}: {e}")
            return False
        except Exception as e:
            print(f"✗ Error reading file {file_path}: {e}")
            return False

    def show_queue_contents(self, max_items=10):
        """Show current queue contents"""
        try:
            if not self.redis_client:
                if not self.connect():
                    return
            
            queue_length = self.redis_client.llen(self.queue_name)
            print(f"\n=== Queue Status ===")
            print(f"Queue: {self.queue_name}")
            print(f"Length: {queue_length}")
            
            if queue_length > 0:
                # Get records from the queue (without removing them)
                items_to_show = min(max_items, queue_length)
                records = self.redis_client.lrange(self.queue_name, 0, items_to_show - 1)
                
                print(f"\nFirst {items_to_show} records:")
                for i, record_str in enumerate(records):
                    try:
                        record = json.loads(record_str)
                        print(f"  {i+1:2d}. ID: {record['customerId']} | Email: {record['email']} | Name: {record['name']}")
                    except json.JSONDecodeError:
                        print(f"  {i+1:2d}. Invalid JSON: {record_str[:50]}...")
                
                if queue_length > max_items:
                    print(f"  ... and {queue_length - max_items} more records")
            else:
                print("Queue is empty")
                
        except Exception as e:
            print(f"Error showing queue contents: {e}")


def create_additional_test_data():
    """Create additional test scenarios"""
    base_time = datetime.utcnow()
    
    # Scenario 1: Rate limiting test with multiple emails
    rate_limit_records = []
    emails = ["alice@test.com", "bob@test.com", "charlie@test.com"]
    
    for email in emails:
        for i in range(7):  # 7 records per email (should trigger rate limiting)
            rate_limit_records.append({
                "customerId": f"rate_{email.split('@')[0]}_{i+1}",
                "name": f"Test User {email.split('@')[0].title()}",
                "email": email,
                "createdAt": (base_time + timedelta(seconds=i*2)).isoformat() + "Z"
            })
    
    # Scenario 2: Burst test - many records at once
    burst_records = []
    for i in range(20):
        burst_records.append({
            "customerId": f"burst_{i+1}",
            "name": f"Burst User {i+1}",
            "email": f"burst{i+1}@example.com",
            "createdAt": (base_time + timedelta(seconds=i)).isoformat() + "Z"
        })
    
    return {
        'rate_limit_test': rate_limit_records,
        'burst_test': burst_records
    }


def main():
    """Main function"""
    print("=== Redis Record Inserter ===")
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python insert_records.py <command> [options]")
        print("\nCommands:")
        print("  basic           - Insert basic test data from test_data.py")
        print("  rate-limit      - Insert data to test rate limiting")
        print("  burst           - Insert burst of records")
        print("  batch           - Insert records in batches with delays")
        print("  json <file>     - Insert records from JSON file")
        print("  status          - Show current queue status")
        print("  clear           - Clear the queue")
        print("\nExamples:")
        print("  python insert_records.py basic")
        print("  python insert_records.py rate-limit")
        print("  python insert_records.py batch")
        print("  python insert_records.py json samples.json")
        print("  python insert_records.py status")
        return
    
    command = sys.argv[1].lower()
    
    # Initialize inserter
    inserter = RedisRecordInserter()
    
    if command == "basic":
        print("Inserting basic test data...")
        records = create_test_data()
        inserter.insert_records(records)
        inserter.show_queue_contents()
        
    elif command == "rate-limit":
        print("Inserting rate limit test data...")
        additional_data = create_additional_test_data()
        inserter.insert_records(additional_data['rate_limit_test'])
        inserter.show_queue_contents()
        
    elif command == "burst":
        print("Inserting burst test data...")
        additional_data = create_additional_test_data()
        inserter.insert_records(additional_data['burst_test'])
        inserter.show_queue_contents()
        
    elif command == "batch":
        print("Inserting records in batches...")
        records = create_test_data()
        inserter.clear_queue()
        inserter.insert_batch_with_delay(records, batch_size=3, delay_seconds=1)
        inserter.show_queue_contents()
        
    elif command == "json":
        if len(sys.argv) < 3:
            print("✗ JSON file path required")
            print("Usage: python insert_records.py json <file_path>")
            return
        
        file_path = sys.argv[2]
        print(f"Inserting records from JSON file: {file_path}")
        if inserter.insert_from_json_file(file_path):
            inserter.show_queue_contents()
        
    elif command == "status":
        inserter.show_queue_contents()
        
    elif command == "clear":
        if inserter.connect():
            inserter.clear_queue()
            inserter.show_queue_contents()
        
    else:
        print(f"Unknown command: {command}")
        print("Use 'python insert_records.py' without arguments to see usage.")


if __name__ == "__main__":
    main()
