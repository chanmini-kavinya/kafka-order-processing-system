import json
import time
import random
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from config import KAFKA_CONFIG, TOPICS, AVRO_SCHEMA_FILE, SCHEMA_REGISTRY_URL, MAX_RETRIES, RETRY_DELAY_MS

class OrderConsumer:
    def __init__(self):
        with open(AVRO_SCHEMA_FILE, 'r') as f:
            schema_str = f.read()
        
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.avro_deserializer = AvroDeserializer(self.schema_registry_client, schema_str)
        
        consumer_config = {
            'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers'],
            'group.id': KAFKA_CONFIG['group.id'],
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        self.consumer = Consumer(consumer_config)
        self.producer = Producer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers']
        })
        
        self.consumer.subscribe([TOPICS['main'], TOPICS['retry']])
        
        self.total_price = 0.0
        self.order_count = 0
        self.running_average = 0.0
        self.retry_counts = {}

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def send_to_dlq(self, original_message, error_reason, key=None):
        dlq_message = {
            'original_message': original_message,
            'error_reason': error_reason,
            'timestamp': time.time()
        }
        
        self.producer.produce(
            TOPICS['dead_letter'],
            key=key or str(original_message.get('orderId', 'unknown')),
            value=json.dumps(dlq_message),
            callback=self.delivery_report
        )
        self.producer.flush()
        
        print(f"Sent to DLQ: {error_reason}")

    def is_retry_message(self, message):
        if message.topic() == TOPICS['retry']:
            try:
                retry_data = json.loads(message.value())
                return True, retry_data
            except:
                pass
        return False, None

    def deserialize_message(self, message):
        if message.topic() == TOPICS['main']:
            ctx = SerializationContext(TOPICS['main'], MessageField.VALUE)
            return self.avro_deserializer(message.value(), ctx)
        return None

    def process_order(self, order):
        order_id = order['orderId']
        price = order['price']
        product = order['product']
        
        if str(price).endswith('.99'):
            raise Exception("Temporary processing failure - price ends with .99")
        
        if price > 900:
            raise ValueError("Price too high - permanent failure")
        
        self.total_price += price
        self.order_count += 1
        self.running_average = self.total_price / self.order_count
        
        print(f"Processed order: {order_id}, Product: {product}, Price: ${price}")
        print(f"Running average price: ${self.running_average:.2f}")
        
        return True

    def handle_retry_message(self, retry_data, message):
        original_message = retry_data['original_message']
        retry_count = retry_data.get('retry_count', 0)
        order_id = original_message['orderId']
        
        print(f"Processing retry attempt {retry_count + 1} for order {order_id}")
        
        try:
            success = self.process_order(original_message)
            if success:
                print(f"Successfully processed retry order {order_id}")
                if order_id in self.retry_counts:
                    del self.retry_counts[order_id]
            return True
            
        except ValueError as e:
            print(f"Permanent failure for retry order {order_id}: {e}")
            self.send_to_dlq(original_message, str(e), message.key())
            return False
            
        except Exception as e:
            print(f"Temporary failure in retry for order {order_id}: {e}")
            
            if retry_count < MAX_RETRIES - 1:  
                new_retry_data = {
                    'original_message': original_message,
                    'retry_count': retry_count + 1,
                    'next_retry_time': time.time() + (RETRY_DELAY_MS / 1000)
                }
                
                self.producer.produce(
                    TOPICS['retry'],
                    key=message.key(),
                    value=json.dumps(new_retry_data),
                    callback=self.delivery_report
                )
                print(f"Sent back to retry topic (attempt {retry_count + 2})")
            else:
                print(f"Max retries exceeded for order {order_id}")
                self.send_to_dlq(original_message, f"Max retries exceeded: {e}", message.key())
            
            return False

    def handle_main_message(self, message):
        order = self.deserialize_message(message)
        if not order:
            return
            
        order_id = order['orderId']
        current_retries = self.retry_counts.get(order_id, 0)
        
        try:
            success = self.process_order(order)
            
            if success:
                print(f"Successfully processed order {order_id}")
                if order_id in self.retry_counts:
                    del self.retry_counts[order_id]
            
        except ValueError as e:
            print(f"Permanent failure for order {order_id}: {e}")
            self.send_to_dlq(order, str(e), message.key())
            
        except Exception as e:
            print(f"Temporary failure for order {order_id}: {e}")
            
            if current_retries < MAX_RETRIES:
                self.retry_counts[order_id] = current_retries + 1
                retry_data = {
                    'original_message': order,
                    'retry_count': current_retries,
                    'next_retry_time': time.time() + (RETRY_DELAY_MS / 1000)
                }
                
                self.producer.produce(
                    TOPICS['retry'],
                    key=message.key(),
                    value=json.dumps(retry_data),
                    callback=self.delivery_report
                )
                print(f"Sent to retry topic (attempt {current_retries + 1})")
            else:
                print(f"Max retries exceeded for order {order_id}")
                self.send_to_dlq(order, f"Max retries exceeded: {e}", message.key())

    def consume_orders(self):
        print("Starting order consumer (main + retry topics)...")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                is_retry, retry_data = self.is_retry_message(msg)
                
                if is_retry:
                    self.handle_retry_message(retry_data, msg)
                else:
                    self.handle_main_message(msg)
                
                self.consumer.commit(msg)
                        
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = OrderConsumer()
    consumer.consume_orders()
    