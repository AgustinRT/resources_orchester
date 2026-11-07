from resources_orchester.domain.exception.domain_exceptions import DomainException


class PersistenceException(DomainException):
    """Raised when an error occurs while interacting with the database."""
    pass

class MessagingException(DomainException):
    """Raised when an error occurs while sending or receiving messages via Kafka."""
    pass
