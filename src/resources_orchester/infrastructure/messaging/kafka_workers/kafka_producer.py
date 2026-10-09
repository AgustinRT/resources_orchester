import json
from typing import Any

from resources_orchester.domain.model.data_resources import DataResources
from ..dtos.message_resource_payload import MessageResourcePayload 

from kafka import KafkaProducer

class WorkerKafkaProducer:
    """
    Implementa el puerto KafkaPortOut, actuando como un Adapter para enviar mensajes reales a Kafka.
    Este componente vive en la capa de infraestructura y contiene la lógica concreta 
    de conexión al broker de Kafka.
    """
    def __init__(self, server_config: dict[str: any]):
        """
        Inicializa el productor de Kafka.

        Args:
            bootstrap_servers: Diccionario de configuración que contiene 'server'.
        """
        print(f"[*] Inicializando Kafka Producer Adapter para brokers: {server_config}")
        self.producer = KafkaProducer(
            bootstrap_servers=server_config["server"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

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
            # 1. Extraer el topic del mensaje de destino
            topic = destiny_message.get('topic') 
            if not topic:
                raise ValueError("destiny_message debe contener la clave 'topic'.")

            message_payload: MessageResourcePayload = MessageResourcePayload.from_data_resources(destiny_message, message)
            # 2. Serializar el objeto DTO (MessageResourcePayload) a un diccionario serializable para Kafka
            message_dict = message_payload.model_dump()
            
            print(f"[INFO] Enviando actualización a tópico '{topic}'...")
            self.producer.send(topic, value=message_dict)
            self.producer.flush() # Forzar el envío inmediato (útil para logs/tests)
            print("[SUCCESS] Mensaje de actualización enviado exitosamente.")
        
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
            if not topic:
                raise ValueError("destiny_message debe contener la clave 'topic'.")

            print(f"[INFO] Enviando solicitud a tópico '{topic}'...")
            self.producer.send(topic, value=message)
            self.producer.flush() 
            print("[SUCCESS] Mensaje de solicitud enviado exitosamente.")
        except Exception as e:
            print(f"[ERROR] Fallo al enviar mensaje de solicitud a '{destiny_message}': {e}")

    def close(self):
        """Cierra la conexión del productor."""
        print("[*] Cerrando el Kafka Producer Adapter.")
        self.producer.close()
