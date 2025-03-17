from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import logging
import time
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Consumer Configuration (Docker-aware)
consumer = KafkaConsumer(
    'ecommerce-events',
    bootstrap_servers='kafka:9092',  # Changed to Docker service name
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='ecommerce-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # Safer manual offset management
    session_timeout_ms=30000,
    heartbeat_interval_ms=10000
)

# MongoDB Connection with error handling
try:
    client = MongoClient(
        'mongodb://mongodb:27017',
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=3000
    )
    client.admin.command('ismaster')  # Test connection
    db = client['ecommerce']
    collection = db['events']
    logger.info("Connected to MongoDB successfully")
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    exit(1)

logger.info("Consumer ready. Waiting for messages...")

try:
    for message in consumer:
        try:
            # Add data validation here
            if not all(key in message.value for key in ['event', 'order_id', 'amount']):
                raise ValueError("Invalid message format")
                
            collection.insert_one({
                **message.value,
                "kafka_offset": message.offset,
                "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            })
            logger.info(f"Inserted order {message.value['order_id']}")
            
            # Commit offset only after successful write
            consumer.commit()
        except Exception as e:
            logger.error(f"Error processing message: {e}")
except KeyboardInterrupt:
    logger.info("Shutting down gracefully...")
finally:
    consumer.close()
    client.close()