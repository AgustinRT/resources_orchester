

import asyncio
import contextlib
import json

from kafka import KafkaConsumer, KafkaProducer
from testcontainers.community.kafka import KafkaContainer

@contextlib.asynccontextmanager
async def rise_kafka_container():
    # The ``with`` block already stops (removes) the container on exit, also on errors.
    # Calling kafka.stop() here as well removed it twice, and Docker answers the second
    # removal with 404 "No such container", which surfaced as a teardown error.
    with KafkaContainer(port=9094) as kafka:
        yield kafka

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

@contextlib.asynccontextmanager
async def tcp_forwarder(listen_port: int, target: str):
    """
    Forwards 127.0.0.1:<listen_port> to <target> ("host:port") while the context is open.
    Simulates "Kafka becomes reachable later": point the bootstrap at a closed port, then open the
    forwarder (after bootstrapping, clients talk to the broker's advertised address directly).
    """
    target_host, target_port = target.rsplit(":", 1)

    async def forward(reader, writer):
        upstream_reader, upstream_writer = await asyncio.open_connection(target_host, int(target_port))

        async def pipe(src, dst):
            with contextlib.suppress(Exception):
                while data := await src.read(65536):
                    dst.write(data)
                    await dst.drain()
            dst.close()

        await asyncio.gather(pipe(reader, upstream_writer), pipe(upstream_reader, writer))

    server = await asyncio.start_server(forward, "127.0.0.1", listen_port)
    try:
        yield server
    finally:
        server.close()
        await server.wait_closed()
