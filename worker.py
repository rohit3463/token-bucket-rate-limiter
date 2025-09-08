#!/usr/bin/env python3
"""
Redis Queue Worker with Rate Limiting and MongoDB Storage

This worker:
1. Pulls batches of records from Redis queue
2. Implements rate limiting (max 5 records per email per minute)
3. Stores valid records in MongoDB
4. Logs rejected records
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import redis
from pymongo import MongoClient
from dateutil import parser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('worker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RedisQueueWorker:
    def __init__(self, 
                 redis_host: str = 'localhost', 
                 redis_port: int = 6379,
                 redis_queue_name: str = 'customer_queue',
                 mongo_host: str = 'localhost',
                 mongo_port: int = 3000,
                 mongo_db_name: str = 'customer_db',
                 mongo_collection_name: str = 'customers',
                 batch_size: int = 10,
                 rate_limit_window_minutes: int = 1,
                 max_records_per_email_per_window: int = 5):
        
        # Redis configuration
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.queue_name = redis_queue_name
        self.batch_size = batch_size
        
        # MongoDB configuration
        self.mongo_client = MongoClient(f'mongodb://admin:password@{mongo_host}:{mongo_port}/')
        self.db = self.mongo_client[mongo_db_name]
        self.collection = self.db[mongo_collection_name]
        
        # Rate limiting configuration
        self.rate_limit_window_minutes = rate_limit_window_minutes
        self.max_records_per_window = max_records_per_email_per_window
        self.email_timestamps_key_prefix = "email_timestamps:"
        
        logger.info("RedisQueueWorker initialized")
        logger.info(f"Redis: {redis_host}:{redis_port}, Queue: {redis_queue_name}")
        logger.info(f"MongoDB: {mongo_host}:{mongo_port}, DB: {mongo_db_name}, Collection: {mongo_collection_name}")
        logger.info(f"Rate limit: {max_records_per_email_per_window} records per {rate_limit_window_minutes} minute(s)")

    def get_batch_from_queue(self) -> List[Dict]:
        """Pull a batch of records from Redis queue"""
        records = []
        try:
            for _ in range(self.batch_size):
                # Use LPOP to get records from the queue
                record_str = self.redis_client.lpop(self.queue_name)
                if record_str is None:
                    break
                
                try:
                    record = json.loads(record_str)
                    records.append(record)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON record: {record_str}, error: {e}")
                    continue
                    
        except redis.RedisError as e:
            logger.error(f"Redis error while fetching batch: {e}")
            
        logger.info(f"Fetched {len(records)} records from queue")
        return records

    def get_email_timestamps(self, email: str) -> List[float]:
        """Get stored timestamps for an email address"""
        key = f"{self.email_timestamps_key_prefix}{email}"
        try:
            timestamps_str = self.redis_client.lrange(key, 0, -1)
            timestamps = [float(ts) for ts in timestamps_str]
            return sorted(timestamps)
        except (redis.RedisError, ValueError) as e:
            logger.error(f"Error getting timestamps for {email}: {e}")
            return []

    def add_email_timestamp(self, email: str, timestamp: float):
        """Add a timestamp for an email address"""
        key = f"{self.email_timestamps_key_prefix}{email}"
        try:
            # Add the new timestamp
            self.redis_client.rpush(key, timestamp)
            
            # Keep only the latest timestamps within the rate limit
            self.redis_client.ltrim(key, -self.max_records_per_window, -1)
            
            # Set expiration for the key (cleanup old data)
            self.redis_client.expire(key, self.rate_limit_window_minutes * 60 + 60)  # Extra 60 seconds buffer
            
        except redis.RedisError as e:
            logger.error(f"Error adding timestamp for {email}: {e}")

    def is_rate_limited(self, email: str, current_timestamp: float) -> bool:
        """Check if email is rate limited"""
        timestamps = self.get_email_timestamps(email)
        
        if len(timestamps) < self.max_records_per_window:
            return False
            
        # Check if we have 5 or more records within the time window
        window_start = current_timestamp - (self.rate_limit_window_minutes * 60)
        recent_timestamps = [ts for ts in timestamps if ts >= window_start]
        
        return len(recent_timestamps) >= self.max_records_per_window

    def clean_old_timestamps(self, email: str, current_timestamp: float):
        """Remove old timestamps outside the rate limit window"""
        key = f"{self.email_timestamps_key_prefix}{email}"
        try:
            timestamps = self.get_email_timestamps(email)
            window_start = current_timestamp - (self.rate_limit_window_minutes * 60)
            
            # Keep only timestamps within the window
            valid_timestamps = [ts for ts in timestamps if ts >= window_start]
            
            # Clear the list and add back only valid timestamps
            self.redis_client.delete(key)
            if valid_timestamps:
                for ts in valid_timestamps:
                    self.redis_client.rpush(key, ts)
                self.redis_client.expire(key, self.rate_limit_window_minutes * 60 + 60)
                
        except redis.RedisError as e:
            logger.error(f"Error cleaning timestamps for {email}: {e}")

    def _is_valid_email(self, email: str) -> bool:
        """Validate email format properly"""
        import re
        
        # Basic email regex pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        # Additional checks
        if not email or not isinstance(email, str):
            return False
            
        # Remove whitespace
        email = email.strip()
        
        # Check basic format with regex
        if not re.match(email_pattern, email):
            return False
            
        # Additional validations
        if email.startswith('@') or email.endswith('@'):
            return False
            
        if '..' in email:  # No consecutive dots
            return False
            
        local_part, domain = email.split('@', 1)
        
        # Local part validations
        if not local_part or len(local_part) > 64:
            return False
            
        if local_part.startswith('.') or local_part.endswith('.'):
            return False
            
        # Domain part validations  
        if not domain or len(domain) > 255:
            return False
            
        if domain.startswith('.') or domain.endswith('.'):
            return False
            
        if domain.startswith('-') or domain.endswith('-'):
            return False
            
        # Must have at least one dot in domain
        if '.' not in domain:
            return False
            
        return True

    def validate_record(self, record: Dict) -> bool:
        """Validate record structure"""
        required_fields = ['customerId', 'name', 'email', 'createdAt']
        
        for field in required_fields:
            if field not in record:
                logger.error(f"Missing required field '{field}' in record: {record}")
                return False
                
        # Validate email format (proper check)
        email = record['email']
        if not self._is_valid_email(email):
            logger.error(f"Invalid email format: {email}")
            return False
            
        # Validate timestamp format
        try:
            parser.parse(record['createdAt'])
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid timestamp format in createdAt: {record['createdAt']}, error: {e}")
            return False
            
        return True

    def store_in_mongodb(self, record: Dict) -> bool:
        """Store record in MongoDB"""
        try:
            # Add processing timestamp
            record['processedAt'] = datetime.utcnow().isoformat() + 'Z'
            
            # Insert the record
            result = self.collection.insert_one(record)
            logger.info(f"Stored record in MongoDB with ID: {result.inserted_id}, customer: {record['email']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store record in MongoDB: {e}, record: {record}")
            return False

    def process_record(self, record: Dict) -> bool:
        """Process a single record"""
        if not self.validate_record(record):
            return False
            
        email = record['email']
        current_timestamp = time.time()
        
        # Clean old timestamps first
        self.clean_old_timestamps(email, current_timestamp)
        
        # Check rate limiting
        if self.is_rate_limited(email, current_timestamp):
            logger.warning(f"Rate limit exceeded for email: {email}. Record rejected: {record}")
            return False
            
        # Store record in MongoDB
        if self.store_in_mongodb(record):
            # Add timestamp to rate limiting tracker
            self.add_email_timestamp(email, current_timestamp)
            logger.info(f"Successfully processed record for email: {email}")
            return True
        else:
            logger.error(f"Failed to store record for email: {email}")
            return False

    def process_batch(self, records: List[Dict]) -> Dict[str, int]:
        """Process a batch of records"""
        stats = {
            'processed': 0,
            'rejected_rate_limit': 0,
            'rejected_invalid': 0,
            'failed_storage': 0
        }
        
        for record in records:
            try:
                if not self.validate_record(record):
                    stats['rejected_invalid'] += 1
                    continue
                    
                email = record['email']
                current_timestamp = time.time()
                
                # Clean old timestamps
                self.clean_old_timestamps(email, current_timestamp)
                
                # Check rate limiting
                if self.is_rate_limited(email, current_timestamp):
                    logger.warning(f"Rate limit exceeded for email: {email}. Record rejected.")
                    stats['rejected_rate_limit'] += 1
                    continue
                    
                # Store record
                if self.store_in_mongodb(record):
                    self.add_email_timestamp(email, current_timestamp)
                    stats['processed'] += 1
                    logger.info(f"Successfully processed record for email: {email}")
                else:
                    stats['failed_storage'] += 1
                    
            except Exception as e:
                logger.error(f"Unexpected error processing record: {e}, record: {record}")
                stats['failed_storage'] += 1
                
        return stats

    def run_worker(self, max_iterations: Optional[int] = None, sleep_interval: int = 1):
        """Run the worker continuously"""
        logger.info("Starting Redis Queue Worker...")
        iteration = 0
        
        try:
            while max_iterations is None or iteration < max_iterations:
                try:
                    # Get batch of records
                    records = self.get_batch_from_queue()
                    
                    if not records:
                        logger.debug("No records in queue, sleeping...")
                        time.sleep(sleep_interval)
                        continue
                    
                    # Process the batch
                    stats = self.process_batch(records)
                    
                    logger.info(f"Batch {iteration + 1} completed - "
                              f"Processed: {stats['processed']}, "
                              f"Rate limited: {stats['rejected_rate_limit']}, "
                              f"Invalid: {stats['rejected_invalid']}, "
                              f"Storage failed: {stats['failed_storage']}")
                    
                    iteration += 1
                    
                    # Short sleep between batches
                    if records:
                        time.sleep(0.1)
                    else:
                        time.sleep(sleep_interval)
                        
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, shutting down...")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error in worker loop: {e}")
                    time.sleep(sleep_interval)
                    
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.redis_client.close()
            self.mongo_client.close()
            logger.info("Worker cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


def main():
    """Main function to run the worker"""
    # Create worker instance
    worker = RedisQueueWorker(
        redis_host='localhost',
        redis_port=6379,
        redis_queue_name='customer_queue',
        mongo_host='localhost',
        mongo_port=3000,  # MongoDB exposed on port 3000 (maps to container's 27017)
        mongo_db_name='customer_db',
        mongo_collection_name='customers',
        batch_size=10,
        rate_limit_window_minutes=1,
        max_records_per_email_per_window=5
    )
    
    # Run the worker
    worker.run_worker()


if __name__ == "__main__":
    main()
