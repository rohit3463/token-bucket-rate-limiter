// MongoDB initialization script
// This script creates the customer_db database and customers collection

print('Starting MongoDB initialization...');

// Switch to the customer_db database
db = db.getSiblingDB('customer_db');

// Create the customers collection with some basic configuration
db.createCollection('customers', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["customerId", "name", "email", "createdAt"],
            properties: {
                customerId: {
                    bsonType: "string",
                    description: "Customer ID must be a string and is required"
                },
                name: {
                    bsonType: "string",
                    description: "Name must be a string and is required"
                },
                email: {
                    bsonType: "string",
                    pattern: "^.+@.+$",
                    description: "Email must be a valid email format and is required"
                },
                createdAt: {
                    bsonType: "string",
                    description: "CreatedAt must be a string and is required"
                },
                processedAt: {
                    bsonType: "string",
                    description: "ProcessedAt must be a string if present"
                }
            }
        }
    }
});

// Create indexes for better performance
db.customers.createIndex({ "email": 1 });
db.customers.createIndex({ "customerId": 1 });
db.customers.createIndex({ "createdAt": 1 });
db.customers.createIndex({ "processedAt": 1 });

// Create a compound index for email and createdAt for rate limiting queries
db.customers.createIndex({ "email": 1, "createdAt": 1 });

print('Created customers collection with validation and indexes');

// Insert a sample document to verify the collection works (will be removed)
var sampleDoc = {
    customerId: "sample",
    name: "Sample User",
    email: "sample@example.com",
    createdAt: new Date().toISOString(),
    processedAt: new Date().toISOString()
};

db.customers.insertOne(sampleDoc);
print('Inserted sample document');

// Remove the sample document
db.customers.deleteOne({ customerId: "sample" });
print('Removed sample document');

// Verify collection exists and show stats
print('Collection stats:');
print(db.customers.stats());

print('MongoDB initialization completed successfully!');
print('Database: customer_db');
print('Collection: customers');
print('Indexes created for: email, customerId, createdAt, processedAt, email+createdAt');
