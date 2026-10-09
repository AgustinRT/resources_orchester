import json
from typing import List
from kafka import KafkaConsumer

class KafkaRequestConsumerService:
    """
    Consumidor especializado para eventos de solicitud/comando (Requests). 
    Maneja temas diseñados para disparar acciones o comandos específicos.
    """

    def __init__(self, bootstrap_servers: list[str], group_id: str = "mcps", topics: List[str] = None):
        """
        Inicializa el consumidor para eventos de solicitud/comando.

        Args:
            bootstrap_servers: Lista de direcciones de brokers de Kafka.
            group_id: El ID del grupo de consumidores ("mcps").
            topics: Tópicos esperados (generalmente solo ['request']).
        """
        self.group_id = group_id
        self.topics = topics or ["request"] # Por defecto, solo escucha el tópico 'request'
        
        # Configuración del consumidor para el grupo "mcps"
        self.consumer = KafkaConsumer(
            *self.topics, 
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id, 
            auto_offset_reset='earliest', # Empezar desde el principio si no hay offset registrado
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializa JSON automáticamente
        )

    def process_message(self, message):
        """
        Procesa un mensaje de Kafka individualmente y trata la carga útil 
        como una solicitud que debe ser procesada por el servicio de orquestación.
        """
        topic = message.topic
        payload = message.value
        print(f"[REQUEST_CONSUME] Tópico: {topic} | Tipo de evento: REQUEST")
        print(f"[REQUEST_CONSUME] Payload recibido (Solicitud): {json.dumps(payload)}")
        # *** AQUÍ SE DEBE LLAMAR AL SERVICIO DE ORQUESTACIÓN PARA EL COMANDO ***
        # Ejemplo: CommandService.execute_request(payload)


    def start_consuming(self):
        """
        Inicia el bucle infinito de consumo y procesamiento, bloqueando la ejecución.
        Debe ejecutarse como un proceso dedicado (daemon/worker).
        """
        print(f"[*] Iniciando Kafka Request Consumer Service para grupo '{self.group_id}' en tópicos: {', '.join(self.topics)}...")
        try:
            for message in self.consumer:
                # Procesa el mensaje de forma atómica (thread-safe)
                self.process_message(message)
        except Exception as e:
            # Captura errores de procesamiento (ej: JSON mal formado) sin detener todo el worker
            print(f"[ERROR FATAL DE CONSUMO] Ocurrió un error crítico durante el consumo: {e}")
        finally:
            self.consumer.close()

# Nota: Ambos servicios deben ejecutarse como procesos independientes y persistentes.
