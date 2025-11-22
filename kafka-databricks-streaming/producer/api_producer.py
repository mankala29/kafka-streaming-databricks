from kafka import KafkaProducer
import json, time, uuid, random

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

actions = ["view", "click", "purchase", "logout"]

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 100),
        "action": random.choice(actions),
        "ts": int(time.time() * 1000)
    }

    producer.send("user-events", event)
    print("Sent:", event)
    time.sleep(1)