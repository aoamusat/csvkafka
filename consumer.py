from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from config import CONFIG
from utils import read_ccloud_config

# Constants
TOPIC_LIST = ["crypto", "transactions"]

# Schema Registry initialization
sr_client = SchemaRegistryClient(CONFIG.get("SCHEMA_REGISTRY"))
crypto_schema = sr_client.get_latest_version("crypto-value")

avro_deserializer = AvroDeserializer(
    sr_client,
    crypto_schema.schema.schema_str,
)

# Confluent Cloud configuration
confluent_cloud_config = read_ccloud_config("client.properties")

confluent_cloud_config["group.id"] = "python-group-1"
confluent_cloud_config["auto.offset.reset"] = "earliest"

# Deserializer Configuration
deserializer_config = confluent_cloud_config | {
    "default.key.deserializer": StringDeserializer(),
    "default.value.deserializer": AvroDeserializer(
        sr_client, crypto_schema.schema.schema_str
    ),
}

# Consumer Configuration
consumer = Consumer(confluent_cloud_config)

# Subscribe to the topic
consumer.subscribe(TOPIC_LIST)

try:
    while True:
        msg = consumer.poll(1.0)  # 1-second timeout
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Process the received message
        key = msg.key().decode("utf-8")
        value = None
        if msg.topic() == "crypto":
            value = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        else:
            value = msg.value()
        print(f"Received message: Key={key}, Value={value}")

except KeyboardInterrupt:
    pass

finally:
    # Close down consumer to commit final offsets.
    consumer.close()
