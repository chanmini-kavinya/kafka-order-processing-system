from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({
    'bootstrap.servers': 'kafka:9092'
})

topic_list = [
    NewTopic("orders", num_partitions=1, replication_factor=1),
    NewTopic("orders-retry", num_partitions=1, replication_factor=1),
    NewTopic("orders-dlq", num_partitions=1, replication_factor=1)
]

fs = admin_client.create_topics(topic_list)

for topic, f in fs.items():
    try:
        f.result() 
        print(f"Topic {topic} created")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")
        