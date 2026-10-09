from typing import Annotated

from fastapi import Depends
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_consumer_update import KafkaUpdateConsumerService
from resources_orchester.infrastructure.messaging.ports.kafka_port_out import KafkaPortOut
from .kafka_workers.kafka_producer import WorkerKafkaProducer

def get_messaging_adapter() -> dict[str, any]:
    """
    Factory function que provee la implementación concreta del Puerto KafkaPortOut 
    para FastAPI's dependency injection (DI).

    Args:
        bootstrap_servers: Lista de direcciones de brokers para inicializar el productor.

    Returns:
        Una instancia de WorkerKafkaProducer implementando KafkaPortOut.
    """

    confic: dict[str, any] = {
        "server": "localhost:9093",
        "group": "mcps"
    }
    return confic


def get_messaging_port_out(server_config: dict[str, any] = Depends(get_messaging_adapter)) -> KafkaPortOut:
    try:
        adapter = WorkerKafkaProducer(server_config=server_config)
        return adapter
    except Exception as e:
        # En caso de falla crítica al conectar, lanzamos la excepción para que FastAPI maneje el error.
        raise RuntimeError(f"Falló la inicialización del adaptador de mensajería (Kafka): {e}")

def active_kafka_consumer_update():
    consumer_update = KafkaUpdateConsumerService()
    if not consumer_update.check_consumer_active():
        consumer_update.initer_consumer_pasive()


MessagingPortOut = Annotated[KafkaPortOut, Depends(get_messaging_port_out)]

