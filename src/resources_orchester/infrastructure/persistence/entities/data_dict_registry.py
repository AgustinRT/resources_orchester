from typing import Dict, List

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.infrastructure.persistence.entities.base import Base
from resources_orchester.infrastructure.persistence.entities.data_field_entity import DataFieldEntity
from sqlalchemy import Enum, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB

class DataRefDict(Base):
    __tablename__ = 'data_dict_registry'

    dictionary_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    data_use_dictionay: Mapped[DataUse] = mapped_column(Enum(DataUse), nullable=False)
    description_data: Mapped[Dict] = mapped_column(JSONB, nullable=False)
    fields: Mapped[List[Dict]] = mapped_column(JSONB, default='[]')

    applications = relationship("RegistryApplications", secondary="link_dict_application")

    def __repr__(self):
        return f"DataRefDict(id={self.dictionary_id}, use='{self.data_use_dictionay}')"