# kafka-order-processing-system
A Kafka-based system for producing and consuming order messages with Avro serialization, real-time price aggregation, retry logic, and a Dead Letter Queue (DLQ).

## Features

- Avro serialization for order messages
- Real-time price aggregation (running average)
- Retry logic for temporary failures
- Dead Letter Queue for permanent failures
- Dockerized for easy setup
- Kafka UI for monitoring

## Quick Start

1. **Start the system**:
   ```bash
   docker-compose up -d
