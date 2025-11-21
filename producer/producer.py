import json
import random
import time
import signal
import threading
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config import KAFKA_CONFIG, TOPICS, AVRO_SCHEMA_FILE, SCHEMA_REGISTRY_URL


class OrderProducer:
    def __init__(self, rate_per_second=1):
        self.running = True
        self.rate = rate_per_second

        with open(AVRO_SCHEMA_FILE, "r") as f:
            schema_str = f.read()

        self.schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.serializer = AvroSerializer(self.schema_registry, schema_str)

        self.producer = Producer({
            "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
            "queue.buffering.max.messages": 100000,
            "linger.ms": 50,
            "batch.num.messages": 1000,
            "enable.idempotence": True
        })

        self.products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
        self.counter = 0

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, *args):
        print("\nShutting down producer gracefully...")
        self.running = False

    def delivery_report(self, err, msg):
        if err:
            print(f"[PRODUCER_ERROR] Delivery failed: {err}")
        else:
            print(f"[PRODUCED] {msg.key().decode()} → {msg.topic()}")

    def generate_order(self):
        self.counter += 1
        return {
            "orderId": f"ORD{self.counter:06d}",
            "product": random.choice(self.products),
            "price": round(random.uniform(10.0, 1000.0), 2)
        }

    def run(self):
        print("Producer started")

        while self.running:
            try:
                order = self.generate_order()
                ctx = SerializationContext(TOPICS["main"], MessageField.VALUE)
                serialized = self.serializer(order, ctx)

                self.producer.produce(
                    TOPICS["main"],
                    key=order["orderId"].encode(),
                    value=serialized,
                    on_delivery=self.delivery_report
                )

                print(f"[PRODUCED] {order}")

                time.sleep(1 / self.rate)
                self.producer.poll(0)

            except Exception as e:
                print(f"[PRODUCER_FATAL_ERROR] {e}")

        self.producer.flush()
        print("Producer shutdown complete.")


if __name__ == "__main__":
    producer = OrderProducer(rate_per_second=0.2)
    producer.run()
