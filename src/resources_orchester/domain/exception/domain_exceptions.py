class DomainException(Exception):
    """Base class for all domain-related exceptions."""
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

class ValidationException(DomainException):
    """Raised when a business rule or data validation fails."""
    pass

class MessageFormatException(DomainException):
    """Raised when the format or content of a message is invalid."""
    pass
