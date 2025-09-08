# Redis Queue Worker with Rate Limiting

A Python-based worker system that processes customer records from a Redis queue with built-in rate limiting and MongoDB storage. The system implements a distributed queue processing pattern with email-based rate limiting to prevent spam and ensure fair processing.

## 🏗️ Architecture

- **Redis Queue**: Stores customer records for processing
- **MongoDB**: Persistent storage for processed records
- **Worker Process**: Processes records with rate limiting (max 5 records per email per minute)
- **Docker Compose**: Containerized infrastructure setup

## 📋 Features

- **Rate Limiting**: Maximum 5 records per email address per minute
- **Batch Processing**: Processes records in configurable batches
- **Data Validation**: Validates email format and required fields
- **Logging**: Comprehensive logging with file and console output
- **Docker Support**: Fully containerized setup
- **Flexible Data Insertion**: Multiple ways to insert test data

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for running worker locally)

### 1. Start Infrastructure

```bash
# Start Redis and MongoDB containers
docker-compose up -d

# Verify containers are running
docker-compose ps
```

### 2. Install Python Dependencies

```bash
# Install required packages
pip install -r requirements.txt
```

### 3. Insert Sample Data

```bash
# Insert basic test data
python insert_records.py basic

# Or insert from JSON file
python insert_records.py json samples.json
```

### 4. Run the Worker

```bash
# Start processing records
python worker.py
```

## 📊 System Components

### Redis Queue (`customer_queue`)
- **Host**: localhost:6379
- **Purpose**: Stores customer records as JSON strings
- **Structure**: FIFO queue (First In, First Out)

### MongoDB (`customer_db.customers`)
- **Host**: localhost:3000 (mapped from container port 27017)
- **Database**: `customer_db`
- **Collection**: `customers`
- **Authentication**: admin/password

### Worker Process
- **Batch Size**: 10 records per batch
- **Rate Limit**: 5 records per email per minute
- **Processing**: Validates, rate-checks, and stores records

## 🔧 Configuration

### Environment Variables

The worker can be configured through environment variables or by modifying the configuration in `worker.py`:

```python
# Redis Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_QUEUE_NAME = 'customer_queue'

# MongoDB Configuration
MONGO_HOST = 'localhost'
MONGO_PORT = 3000
MONGO_DB_NAME = 'customer_db'
MONGO_COLLECTION_NAME = 'customers'

# Rate Limiting
RATE_LIMIT_WINDOW_MINUTES = 1
MAX_RECORDS_PER_EMAIL_PER_WINDOW = 5
BATCH_SIZE = 10
```

## 📥 Data Insertion

### Using insert_records.py

The `insert_records.py` script provides multiple ways to insert test data:

#### Basic Commands

```bash
# Insert basic test data (14 records with rate limiting scenarios)
python insert_records.py basic

# Insert records to test rate limiting (multiple records per email)
python insert_records.py rate-limit

# Insert burst of records (20 unique emails)
python insert_records.py burst

# Insert records in batches with delays
python insert_records.py batch

# Insert records from JSON file
python insert_records.py json <file_path>

# Check queue status
python insert_records.py status

# Clear the queue
python insert_records.py clear
```

#### Examples

```bash
# Insert sample data from the provided samples.json
python insert_records.py json samples.json

# Insert test data from test_data.json
python insert_records.py json test_data.json

# Insert basic test data and view queue status
python insert_records.py basic
```

### JSON File Format

JSON files should contain an array of customer records:

```json
[
  {
    "customerId": "1",
    "name": "John Doe",
    "email": "john@example.com",
    "createdAt": "2024-03-26T12:00:00Z"
  },
  {
    "customerId": "2",
    "name": "Jane Smith", 
    "email": "jane@example.com",
    "createdAt": "2024-03-26T12:01:00Z"
  }
]
```

### Using test_data.py (Legacy)

```bash
# Populate queue with test data
python test_data.py populate

# Check queue status
python test_data.py status
```

## 🧪 Testing

### Manual Testing

1. **Start the infrastructure**:
   ```bash
   docker-compose up -d
   ```

2. **Insert test data**:
   ```bash
   python insert_records.py rate-limit
   ```

3. **Run the worker**:
   ```bash
   python worker.py
   ```

4. **Monitor logs**:
   - Console output shows real-time processing
   - `worker.log` file contains detailed logs

### Test Scenarios

#### Rate Limiting Test
```bash
python insert_records.py rate-limit
```
This inserts multiple records with the same email addresses to test the rate limiting functionality.

#### Burst Processing Test
```bash
python insert_records.py burst
```
This inserts 20 records with unique emails to test batch processing capabilities.

#### Batch Processing Test
```bash
python insert_records.py batch
```
This inserts records in small batches with delays to simulate real-world conditions.

### Running Unit Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_worker.py -v

# Run with coverage
python -m pytest tests/ --cov=. --cov-report=html
```

### Automated Testing Script

You can run the provided test script:

```bash
# Make sure the infrastructure is running
docker-compose up -d

# Run automated tests
python test_data.py
```

## 📈 Monitoring

### Queue Status
```bash
# Check current queue length and contents
python insert_records.py status
```

### Worker Logs
```bash
# View real-time logs
tail -f worker.log

# Search for specific patterns
grep "Rate limit" worker.log
grep "Successfully processed" worker.log
```

### MongoDB Data
```bash
# Connect to MongoDB container
docker exec -it mongodb-customer mongosh -u admin -p password

# Use customer database
use customer_db

# Count processed records
db.customers.countDocuments()

# Find records by email
db.customers.find({"email": "john@example.com"})

# Check recent records
db.customers.find().sort({"processedAt": -1}).limit(5)
```

### Redis Queue
```bash
# Connect to Redis container
docker exec -it redis-queue redis-cli

# Check queue length
LLEN customer_queue

# View queue contents (first 5 items)
LRANGE customer_queue 0 4

# Check rate limiting data
KEYS email_timestamps:*
```

## 📝 Data Schema

### Customer Record Structure
```json
{
  "customerId": "string (required)",
  "name": "string (required)", 
  "email": "string (required, valid email format)",
  "createdAt": "string (required, ISO 8601 format)",
  "processedAt": "string (added by worker)"
}
```

### MongoDB Collection
- **Database**: `customer_db`
- **Collection**: `customers`
- **Indexes**: email, customerId, createdAt, processedAt, email+createdAt

## 🐛 Troubleshooting

### Common Issues

1. **Redis Connection Error**
   ```bash
   # Check if Redis is running
   docker-compose ps
   # Restart Redis if needed
   docker-compose restart redis
   ```

2. **MongoDB Connection Error**
   ```bash
   # Check MongoDB status
   docker-compose logs mongodb
   # Test connection
   docker exec -it mongodb-customer mongosh -u admin -p password
   ```

3. **Worker Not Processing Records**
   ```bash
   # Check queue status
   python insert_records.py status
   # Check worker logs
   tail -f worker.log
   ```

4. **Rate Limiting Not Working**
   ```bash
   # Check Redis for rate limiting keys
   docker exec -it redis-queue redis-cli KEYS "email_timestamps:*"
   ```

### Performance Issues

- **Slow Processing**: Increase `batch_size` in worker configuration
- **High Memory Usage**: Decrease `batch_size` or implement memory limits
- **Rate Limiting Too Strict**: Adjust `max_records_per_email_per_window` or `rate_limit_window_minutes`

## 🔄 Development Workflow

1. **Make changes to worker logic**
2. **Stop the worker** (Ctrl+C)
3. **Clear queue for fresh test**:
   ```bash
   python insert_records.py clear
   ```
4. **Insert test data**:
   ```bash
   python insert_records.py basic
   ```
5. **Restart worker**:
   ```bash
   python worker.py
   ```

## 📚 File Structure

```
├── docker-compose.yml      # Infrastructure setup
├── worker.py              # Main worker process
├── insert_records.py      # Data insertion utility
├── test_data.py          # Legacy test data generator
├── requirements.txt      # Python dependencies
├── samples.json         # Sample JSON data
├── test_data.json       # Test data file
├── init-mongo.js        # MongoDB initialization
├── init-redis.sh        # Redis initialization
├── worker.log           # Worker process logs
└── README.md           # This file
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run existing tests to ensure nothing breaks
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
