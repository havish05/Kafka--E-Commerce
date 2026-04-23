import json, random, time, uuid
from kafka import KafkaProducer
from datetime import datetime, timedelta

BOOTSTRAP_SERVERS = "host.docker.internal:29092"
Topic_NAME = "raw_events"

producer = KafkaProducer(
    bootstrap_servers= BOOTSTRAP_SERVERS,
    key_serializer = lambda k: k.encode('utf-8') if k else None,
    value_serializer = lambda v: json.dumps(v).encode('utf-8') if v else None,
)

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]

def random_timestamp_last_6_days():
    now = datetime.utcnow()
    past_time = now - timedelta(days=6)
    random_seconds =  random.uniform(0, (now - past_time).total_seconds())
    return past_time + timedelta(seconds=random_seconds)

def generate_event():
    is_invalid = random.random() < 0.25
    cust_id = f"CUST_{random.randint(1,10)}"

    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10,500), 2)
    currency = "INR"

    invalid_field = None

    if is_invalid:
        invalid_field = random.choice(['customer_id', 'event_type', 'amount', 'currency'])

    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": cust_id if invalid_field != 'customer_id' else None,
        "event_type": event_type if invalid_field != 'event_type' else random.choice(INVALID_EVENT_TYPES),
        "amount": amount if invalid_field != 'amount' else -amount,
        "currency": currency if invalid_field != 'currency' else None,
        "event_timestamp": random_timestamp_last_6_days().isoformat(),
        "is_valid": not is_invalid,
        "invalid_field": invalid_field
    }

    return event['customer_id'], event

print("Starting Kafka production...")

while True:
    key, event = generate_event()

    producer.send(
        topic=Topic_NAME,
        key=key,
        value=event
    )

    print(f"Produced event | key = {key} | valid = {event['is_valid']}")

    time.sleep(1)
