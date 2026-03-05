import json
import time
import uuid
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS

if random.random() < 0.2:
    event_time = datetime.utcnow() - timedelta(minutes=5)
else:
    event_time = datetime.utcnow()

# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_TOPIC = "ride_events"
KAFKA_SERVER = "127.0.0.1:9092"

# Change this mode:
# "normal"  -> all current timestamps
# "late"    -> all events 5 minutes old
# "mixed"   -> 80% current, 20% late
MODE = "mixed"

cities = ["NYC", "Chicago", "Seattle", "SF"]

# --------------------------------------------------
# Kafka Producer
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Starting ride event generator...")

# --------------------------------------------------
# Event Loop
# --------------------------------------------------

while True:

    # Decide event_time based on mode
    if MODE == "normal":
        event_time = datetime.utcnow()

    elif MODE == "late":
        event_time = datetime.utcnow() - timedelta(minutes=5)

    elif MODE == "mixed":
        # 20% chance of late event
        if random.random() < 0.2:
            event_time = datetime.utcnow() - timedelta(minutes=5)
        else:
            event_time = datetime.utcnow()

    else:
        event_time = datetime.utcnow()

    event = {
        "ride_id": str(uuid.uuid4()),
        "city": random.choice(cities),
        "fare": round(random.uniform(5, 50), 2),
        "event_time": event_time.isoformat()
    }

    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()

    print("Sent:", event)

    time.sleep(0.5)

