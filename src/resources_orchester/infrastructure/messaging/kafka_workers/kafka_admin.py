import json
import os
from typing import Dict, List

from dotenv import dotenv_values
from pathlib import Path
from kafka.admin import KafkaAdminClient, NewTopic

class WorkerKafkaAdmin:
    """
    Implementa el puerto KafkaPortOut, actuando como un Adapter para enviar mensajes reales a Kafka.
    Este componente vive en la capa de infraestructura y contiene la lógica concreta 
    de conexión al broker de Kafka.
    """
    def __init__(self, env_use: str = "dev"):
        """
        Inicializa el productor de Kafka.

        Args:
            bootstrap_servers: Diccionario de configuración que contiene 'server'.
        """

        env_path = str([f for f in (Path(os.path.abspath("")).rglob(f".env.messaging.{env_use}")) if f.is_file()][0])
        
        self._messagin_config: Dict[str, str] = dotenv_values(env_path)
        bootstrap = self._messagin_config['KAFKA_BOOTSTRAP_SERVERS'] if "KAFKA_BOOTSTRAP_SERVERS" in self._messagin_config else os.getenv("bootstrap_servers")
        admin_client = KafkaAdminClient(
             bootstrap_servers=bootstrap,

        )

        topics: List[str] = [x for x in self._messagin_config['KAFKA_PRODUCER_TOPICS'].split(",")] + [x for x in self._messagin_config['KAFKA_CONSUMER_TOPICS'].split(",")]
        topic_list = []
        for t in topics:
            topic = NewTopic(name=t, num_partitions=1, replica_assignments=1)
            topic_list.append(topic)

        try:
            admin_client.create_topics([NewTopic(name="update", num_partitions=1, replica_assignments=1)],
                                       validate_only=False)
        except Exception as e:
            print(e)
            raise

