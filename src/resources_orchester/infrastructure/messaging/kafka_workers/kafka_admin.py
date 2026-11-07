import logging
from typing import List

from kafka.admin import KafkaAdminClient, NewTopic

from resources_orchester.infrastructure.bootstrap.settings import KafkaSettings

logger = logging.getLogger(__name__)


class WorkerKafkaAdmin:
    """
    Kafka administration helper: creates the topics this service produces to and consumes from.
    Configuration is injected (KafkaSettings); nothing is read from files or the environment.
    """

    def __init__(self, settings: KafkaSettings, num_partitions: int = 1, replication_factor: int = 1):
        self._settings = settings
        self._num_partitions = num_partitions
        self._replication_factor = replication_factor

    def ensure_topics(self) -> List[str]:
        """Creates every configured producer/consumer topic that does not exist yet. Returns the created ones."""
        wanted = list(dict.fromkeys(self._settings.producer_topics + self._settings.consumer_topics))
        admin_client = KafkaAdminClient(bootstrap_servers=self._settings.bootstrap_servers)
        try:
            missing = [topic for topic in wanted if topic not in set(admin_client.list_topics())]
            if missing:
                admin_client.create_topics(
                    [NewTopic(name=topic, num_partitions=self._num_partitions, replication_factor=self._replication_factor)
                     for topic in missing],
                    validate_only=False)
                logger.info("Kafka topics created: %s", ", ".join(missing))
            return missing
        finally:
            admin_client.close()
