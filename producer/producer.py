import json
import random
import time
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import avro.schema
from config import KAFKA_CONFIG, TOPICS, AVRO_SCHEMA_FILE, SCHEMA_REGISTRY_URL

class OrderProducer:
    def __init__(self):
        with open(AVRO_SCHEMA_FILE, 'r') as f:
            schema_str = f.read()
        
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.avro_serializer = AvroSerializer(self.schema_registry_client, schema_str)
        
        producer_config = {
            'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers']
        }
        
        self.producer = Producer(producer_config)
        self.products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
        self.produced_count = 0

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def generate_order(self):
        self.produced_count += 1
        return {
            'orderId': f"ORD{self.produced_count:06d}",
            'product': random.choice(self.products),
            'price': round(random.uniform(10.0, 1000.0), 2)
        }

    def produce_orders(self):
        print("Starting order producer...")
        
        try:
            while True:
                order = self.generate_order()
                
                ctx = SerializationContext(TOPICS['main'], MessageField.VALUE)
                serialized_order = self.avro_serializer(order, ctx)
                
                self.producer.produce(
                    topic=TOPICS['main'],
                    value=serialized_order,
                    callback=self.delivery_report
                )
                
                print(f"Produced order: {order}")
                
                time.sleep(3)
                
                if self.produced_count % 5 == 0:
                    self.producer.flush()
                    
        except KeyboardInterrupt:
            print("Stopping producer...")
        except Exception as e:
            print(f"Producer error: {e}")
        finally:
            self.producer.flush()

if __name__ == "__main__":
    producer = OrderProducer()
    producer.produce_orders()
    