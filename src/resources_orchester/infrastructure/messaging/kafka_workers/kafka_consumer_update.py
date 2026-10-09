import json
import threading
from kafka import KafkaConsumer
import os

class KafkaUpdateConsumerService:
    """
    Consumidor especializado para eventos de cambio de estado (CRUD). 
    Maneja temas como 'update', 'delete' y 'create'.
    Asume que la lógica de negocio debe manejar el tipo de evento.
    """

    _consumer_thread = None

    def __init__(self):
        """
        Inicializa el consumidor para eventos de actualización/estado.
        """
        self.group_id = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "mcps")
        self.topics = [t.strip() for t in os.environ.get("KAFKA_CONSUMER_TOPICS", "update,delete,create").split(',')]
        self.bootstrap_servers = [os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")]

        self.consumer = KafkaConsumer(
            *self.topics, 
            bootstrap_servers=self.bootstrap_servers, 
            group_id=self.group_id, 
            auto_offset_reset='earliest', # Empezar desde el principio si no hay offset registrado
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializa JSON automáticamente
        )

    def check_consumer_active(self):
        return False if not self._consumer_thread else True

    def initer_consumer_pasive(self):
        self._consumer_thread = threading.Thread(target=self.start_consuming)
        self._consumer_thread.daemon = True
        self._consumer_thread.start()

    def process_message(self, message):
        """
        Procesa un mensaje de Kafka individualmente y determina el tipo de evento.
        Debe invocar la lógica de negocio apropiada basada en el tópico.
        """
        topic = message.topic
        payload = message.value
        event_type = ""

        if topic in ["update", "delete", "create"]:
            # Aquí se puede extraer un tipo específico del payload o asumirlo por el tópico
            event_type = topic.upper()
            print(f"[UPDATE_CONSUME] Tópico: {topic} | Tipo de evento: {event_type}")
            print(f"[UPDATE_CONSUME] Payload recibido: {json.dumps(payload)}")
            # *** AQUÍ SE DEBE LLAMAR AL SERVICIO DE DOMINIO PARA EL EVENTO ESPECÍFICO ***
            # Ejemplo: EventService.handle_update(payload)

        else:
            print(f"[WARNING] Tópico desconocido o no gestionado en este consumidor: {topic}")


    def start_consuming(self):
        """
        Inicia el bucle infinito de consumo y procesamiento, bloqueando la ejecución.
        Debe ejecutarse como un proceso dedicado (daemon/worker).
        """
        print(f"[*] Iniciando Kafka Update Consumer Service para grupo '{self.group_id}' en tópicos: {', '.join(self.topics)}...")
        try:
            for message in self.consumer:
                self.process_message(message)

        except Exception as e:
            print(f"[ERROR FATAL DE CONSUMO] Ocurrió un error crítico durante el consumo: {e}")
        finally:
            self.consumer.close()
