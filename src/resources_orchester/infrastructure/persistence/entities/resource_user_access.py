from typing import List, Optional

from pgvector.sqlalchemy import VECTOR
from sqlalchemy import BigInteger, ForeignKey, Integer, Sequence, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from resources_orchester.infrastructure.persistence.entities.base import Base

# Same sequence that database/init.sql uses as the column default.
RESOURCE_USER_ACCESS_SEQ = Sequence("resource_user_access_seq")


class ResourceUserAccess(Base):
    """
    Table ``resource_user_access``: one row per access of a user to a resource, with the embedding
    of the prompt. ``predicate_embed`` has no fixed dimension (it depends on ``model_name``), and
    both are NULL or both are set (check constraint in init.sql).
    """
    __tablename__ = "resource_user_access"

    id: Mapped[int] = mapped_column(BigInteger, RESOURCE_USER_ACCESS_SEQ, primary_key=True,
                                    server_default=RESOURCE_USER_ACCESS_SEQ.next_value())
    username: Mapped[str] = mapped_column(String(150), nullable=False)
    resource_id: Mapped[int] = mapped_column(Integer, ForeignKey("data_dict_registry.dictionary_id"), nullable=False)
    predicate: Mapped[str] = mapped_column(Text, nullable=False)
    predicate_embed: Mapped[Optional[List[float]]] = mapped_column(VECTOR(), nullable=True)
    model_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    def __repr__(self):
        return f"ResourceUserAccess(id={self.id}, username='{self.username}', resource_id={self.resource_id})"
