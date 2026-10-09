

import contextlib
import json

from kafka import KafkaConsumer, KafkaProducer
from testcontainers.community.kafka import KafkaContainer

# image: str = "confluentinc/cp-kafka:7.6.0",
# port: int = 9094,

@contextlib.asynccontextmanager
async def rise_kafka_container():
    with KafkaContainer(port=9094) as kafka:
        try:
            yield kafka
        except Exception as e:
            print(e)
            kafka.stop()
            raise
        finally:
            kafka.stop()

def consumer_tester(bootstrap, topic) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    return consumer

def producer_tester(bootstrap) -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer