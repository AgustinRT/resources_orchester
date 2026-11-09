from resources_orchester.domain.exception.domain_exceptions import DomainException


class PersistenceException(DomainException):
    """Raised when an error occurs while interacting with the database."""
    pass

class MessagingException(DomainException):
    """Raised when an error occurs while sending or receiving messages via Kafka."""
    pass

class EmbeddingException(DomainException):
    """
    Raised when the embedding of a text cannot be obtained from the AI model (provider down or
    unreachable, timeout, rejected credentials, invalid response or missing configuration).
    Operations that need the embedding fail as a whole: nothing is stored.
    """
    pass
