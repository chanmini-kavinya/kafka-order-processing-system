import os

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BROKER', 'kafka:9092'),
    'group.id': 'order-processor-group'
}

TOPICS = {
    'main': 'orders',
    'retry': 'orders-retry',
    'dead_letter': 'orders-dlq'
}

SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
AVRO_SCHEMA_FILE = '/app/schemas/order.avsc'

MAX_RETRIES = 3
RETRY_DELAY_MS = 5000
