class PersistenceException(DomainException):
    """Raised when an error occurs while interacting with the database."""
    pass

class MessagingException(DomainException):
    """Raised when an error occurs while sending or receiving messages via Kafka."""
    pass
