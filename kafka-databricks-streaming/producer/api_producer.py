from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import time, uuid, random

schema_str = open("/Users/swetamankala/Desktop/Kafka/kafka-databricks-streaming/schema/user_event.avsc").read()

sr = SchemaRegistryClient({"url": "http://localhost:8081"})
value_serializer = AvroSerializer(sr, schema_str)

producer = SerializingProducer({
    "bootstrap.servers": "localhost:9092",
    "value.serializer": value_serializer
})

actions = ["login", "logout", "purchase", "view"]

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": str(random.randint(1, 50)),
        "action": random.choice(actions),
        "timestamp": int(time.time() * 1000)
    }

    producer.produce(topic="user-events", value=event)
    producer.flush()

    print("Sent:", event)
    time.sleep(1)