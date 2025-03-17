from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=100,  
    acks='all',  # Ensure message acknowledgment
    retries=3  # Add retries for reliability
)

events = ["order_placed", "order_shipped", "order_delivered", "order_cancelled"]

while True:
    event_data = {
        "event": random.choice(events),
        "order_id": random.randint(1000, 9999),
        "amount": round(random.uniform(50, 500), 2),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    try:
        future = producer.send('ecommerce-events', value=event_data)
        result = future.get(timeout=30) 
        print(f"Success: {event_data}")
    except Exception as e:
        print(f"Failed: {e}")
        

    time.sleep(random.uniform(0.5, 2.5))  #realistic random interval
