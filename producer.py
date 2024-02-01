from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from config import CONFIG
from utils import delivery_callback, read_ccloud_config, read_transaction_file
from datetime import datetime
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

sr_client = SchemaRegistryClient(CONFIG.get("SCHEMA_REGISTRY"))

TOPIC_NAME = "crypto"

# Note that the schema name on confluent cloud is crypto
crypto_schema = sr_client.get_latest_version("crypto-value")

avro_serializer = AvroSerializer(sr_client, crypto_schema.schema.schema_str)

confluent_cloud_config = read_ccloud_config("client.properties")

# Use this configuration when using SerializingProducer
# Note that serializing producer is experimental
# producer_config = confluent_cloud_config | {
#     "key.serializer": StringSerializer(),
#     "value.serializer": AvroSerializer(sr_client, crypto_schema.schema.schema_str),
# }

producer = Producer(confluent_cloud_config)

for index, tx in enumerate(read_transaction_file("transactions.csv")):
    key = str(index) + str(datetime.now().timestamp()).split(".")[0]
    try:
        value = avro_serializer(tx, SerializationContext(TOPIC_NAME, MessageField.VALUE))
        producer.produce(
            TOPIC_NAME, key=f"{key}", value=value, on_delivery=delivery_callback
        )
    except ValueError:
        print("Invalid input, discarding record...")
        continue
    except KeyboardInterrupt:
        break
    producer.poll()

producer.flush()
