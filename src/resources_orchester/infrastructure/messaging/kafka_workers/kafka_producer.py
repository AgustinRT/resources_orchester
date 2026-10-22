import asyncio
import json
import os
from typing import Any, Dict, List

from aiokafka import AIOKafkaProducer
from dotenv import dotenv_values
from pathlib import Path
from resources_orchester.domain.model.data_resources import DataResources
from ..dtos.message_resource_payload import MessageResourcePayload 

from kafka import KafkaProducer

class WorkerKafkaProducer:
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
        self._bootstrap = self._messagin_config['KAFKA_BOOTSTRAP_SERVERS'] if "KAFKA_BOOTSTRAP_SERVERS" in self._messagin_config else os.getenv("bootstrap_servers")
        self._topics: List[str] = [x for x in self._messagin_config['KAFKA_PRODUCER_TOPICS'].split(",")]

    
    async def init_producer(self):
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap,
                                          value_serializer=lambda x: json.loads(x.decode('utf-8')))
        await self._producer.start()

    def send_message_to_update(self, destiny_message: dict[str, Any], message: DataResources) -> None:
        """
        Envía un mensaje de actualización a un destino específico (Topic), 
        utilizando el DTO estructurado.
        
        Args:
            destiny_message (dict): El identificador del destino (el tópico en Kafka). 
                                    Debe contener al menos la clave 'topic'.
            message (MessageResourcePayload): La carga útil tipada con MessageResourcePayload.
        """
        try:
            topic = destiny_message.get('topic') 
            if not topic or topic not in self._topics:
                raise ValueError("destiny_message debe contener la clave 'topic'.")

            message_payload: MessageResourcePayload = MessageResourcePayload.from_data_resources(destiny_message, message)
            message_dict = message_payload.model_dump()
            
            self._producer.send(topic, value=message_dict)
            self._producer.flush() # Forzar el envío inmediato (útil para logs/tests)

        except Exception as e:
            print(f"[ERROR] Fallo al enviar mensaje de actualización a '{destiny_message}': {e}")

    def send_message_to_request(self, destiny_message: dict[str, Any], message: dict[str, Any]) -> None:
        """
        Envía un mensaje para solicitar una acción (evento de solicitud) a un destino específico.
        (Esta función mantiene su firma original ya que no fue modificada en este paso).

        Args:
            destiny_message (dict): El identificador del destino (el tópico en Kafka). 
                                    Debe contener al menos la clave 'topic'.
            message (dict): La carga útil con los datos y parámetros de la solicitud.
        """
        try:
            # Asumimos que el topic está anidado o es fácilmente accesible
            topic = destiny_message.get('topic')
            if not topic or topic not in self._topics:
                raise ValueError("destiny_message debe contener la clave 'topic'.")

            print(f"[INFO] Enviando solicitud a tópico '{topic}'...")
            self._producer.send(topic, value=message)
            self._producer.flush() 
            print("[SUCCESS] Mensaje de solicitud enviado exitosamente.")
        except Exception as e:
            print(f"[ERROR] Fallo al enviar mensaje de solicitud a '{destiny_message}': {e}")

    def close(self):
        """Cierra la conexión del productor."""
        self._producer.stop()
