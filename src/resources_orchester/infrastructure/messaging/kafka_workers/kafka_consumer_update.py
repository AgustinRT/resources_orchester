import asyncio
import json
import threading
from typing import Dict
from aiokafka import AIOKafkaConsumer
from dotenv import dotenv_values
from pathlib import Path
from kafka import KafkaConsumer
import os

class KafkaUpdateConsumerService:
    """
    Consumidor especializado para eventos de cambio de estado (CRUD). 
    Maneja temas como 'update', 'delete' y 'create'.
    Asume que la lógica de negocio debe manejar el tipo de evento.
    """

    def __init__(self, env_use: str = "dev"):
        """
        Inicializa el consumidor para eventos de actualización/estado.
        """
        env_path = str([f for f in (Path(os.path.abspath("")).rglob(f".env.messaging.{env_use}")) if f.is_file()][0])
        self._messagin_config: Dict[str, str] = dotenv_values(env_path)
        self._bootstrap = self._messagin_config['KAFKA_BOOTSTRAP_SERVERS'] if "KAFKA_BOOTSTRAP_SERVERS" in self._messagin_config else os.getenv("bootstrap_servers")

    async def init_consumer(self):
        self._topics_consumer = [ x for x in self._messagin_config['KAFKA_CONSUMER_TOPICS'].split(",")]
        self._consumer = AIOKafkaConsumer(self._topics_consumer[0],
                                          bootstrap_servers=self._bootstrap,
                                          value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        await self._consumer.start()
        asyncio.create_task(self.consumer_messager())

    async def consumer_messager(self):
        async for msg in self._consumer:
            self.process_message(msg)

    def process_message(self, message):
        """
        Procesa un mensaje de Kafka individualmente y determina el tipo de evento.
        Debe invocar la lógica de negocio apropiada basada en el tópico.
        """
        topic = message.topic
        payload = message.value
        event_type = ""

        if topic in self._topics_consumer:
            event_type = topic.upper()
            print(f"[UPDATE_CONSUME] Tópico: {topic} | Tipo de evento: {event_type}")
            print(f"[UPDATE_CONSUME] Payload recibido: {json.dumps(payload)}")

        else:
            print(f"[WARNING] Tópico desconocido o no gestionado en este consumidor: {topic}")


    def start_consuming(self):
        """
        Inicia el bucle infinito de consumo y procesamiento, bloqueando la ejecución.
        Debe ejecutarse como un proceso dedicado (daemon/worker).
        """
        print(f"[*] Iniciando Kafka Update Consumer Service para grupo ···· en tópicos: {', '.join(self._topics_consumer)}...")
        try:
            for message in self._consumer:
                self.process_message(message)

        except Exception as e:
            print(f"[ERROR FATAL DE CONSUMO] Ocurrió un error crítico durante el consumo: {e}")
        finally:
            self._consumer.stop()

    def stop_consuming(self):
        """
        Inicia el bucle infinito de consumo y procesamiento, bloqueando la ejecución.
        Debe ejecutarse como un proceso dedicado (daemon/worker).
        """
        print(f"[*] Iniciando Kafka Update Consumer Service para grupo ···· en tópicos: {', '.join(self._topics_consumer)}...")
        try:
            self._consumer.stop()
        except Exception as e:
            print(f"[ERROR FATAL DE CONSUMO] Ocurrió un error crítico durante el consumo: {e}")
        finally:
            self._consumer.stop()
