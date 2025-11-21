import json
import time
from threading import Thread
from flask import Flask, jsonify, render_template
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from config import KAFKA_CONFIG, TOPICS, AVRO_SCHEMA_FILE, SCHEMA_REGISTRY_URL

app = Flask(__name__)

metrics = {
    'total_orders_from_main': 0,
    'running_average': 0.0,
    'success_count': 0,
    'retry_count': 0,
    'dlq_count': 0,
    'recent_prices': [],
    'total_price_sum': 0.0,
    'last_updated': None,
    'seen_order_ids': set()
}

with open(AVRO_SCHEMA_FILE, 'r') as f:
    schema_str = f.read()

schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str, lambda obj, ctx: obj)

class KafkaMetricsConsumer:
    def __init__(self):
        consumer_config = {
            'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers'],
            'group.id': 'metrics-dashboard',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([TOPICS['main'], TOPICS['retry'], TOPICS['dead_letter']])
        self.running = True

    def consume_metrics(self):
        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            topic = msg.topic()

            if topic == TOPICS['main']:
                try:
                    order = avro_deserializer(msg.value(), None)
                    if order and 'price' in order and 'orderId' in order:
                        order_id = order['orderId']
                        price = order['price']
                        
                        if order_id not in metrics['seen_order_ids']:
                            metrics['seen_order_ids'].add(order_id)
                            metrics['total_orders_from_main'] += 1
                            metrics['recent_prices'].append(price)
                            metrics['total_price_sum'] += price
                except Exception as e:
                    continue
                    
            elif topic == TOPICS['retry']:
                try:
                    retry_data = json.loads(msg.value().decode('utf-8'))
                    if 'original_message' in retry_data:
                        metrics['retry_count'] += 1
                except:
                    metrics['retry_count'] += 1
                    
            elif topic == TOPICS['dead_letter']:
                try:
                    dlq_data = json.loads(msg.value().decode('utf-8'))
                    if 'original_message' in dlq_data:
                        original_order = dlq_data['original_message']
                        if 'orderId' in original_order and 'price' in original_order:
                            order_id = original_order['orderId']
                            price = original_order['price']
                            
                            if price in metrics['recent_prices']:
                                metrics['recent_prices'].remove(price)
                                metrics['total_price_sum'] -= price
                            
                        metrics['dlq_count'] += 1
                except:
                    metrics['dlq_count'] += 1

            metrics['success_count'] = metrics['total_orders_from_main'] - metrics['dlq_count']
            
            metrics['recent_prices'] = metrics['recent_prices'][-20:]
            
            if metrics['success_count'] > 0:
                metrics['running_average'] = metrics['total_price_sum'] / metrics['success_count']
                
            metrics['last_updated'] = time.time()

    def stop(self):
        self.running = False
        self.consumer.close()

kafka_metrics = KafkaMetricsConsumer()
thread = Thread(target=kafka_metrics.consume_metrics)
thread.daemon = True
thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time(),
        'message': 'Dashboard is running and reading metrics from Kafka'
    })

@app.route('/metrics')
def get_metrics():
    total_processed = metrics['total_orders_from_main']
    success_rate = (metrics['success_count'] / total_processed * 100) if total_processed > 0 else 0
    
    return jsonify({
        'total_orders': total_processed,
        'running_average': round(metrics['running_average'], 2),
        'success_count': metrics['success_count'],
        'retry_count': metrics['retry_count'],
        'dlq_count': metrics['dlq_count'],
        'recent_prices': metrics['recent_prices'],
        'success_rate': round(success_rate, 1),
        'last_updated': metrics['last_updated']
    })

if __name__ == '__main__':
    print("Starting Kafka Metrics Dashboard...")
    app.run(host='0.0.0.0', port=5000, debug=False)
