from typing import List

from resources_orchester.infrastructure.persistence.entities.base import Base
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, registry, relationship

class DataDictionaryAppRef(Base):
    __tablename__ = 'link_dict_application'

    dictionary_id: Mapped[int]  = mapped_column(Integer, ForeignKey('data_dict_registry.dictionary_id'), primary_key=True)
    application_id: Mapped[int] = mapped_column(Integer, ForeignKey('registry_applications.application_id'), primary_key=True)

    def __repr__(self):
        return f"DataDictionaryAppRef(dict_id={self.dictionary_id}, app_id={self.application_id})"