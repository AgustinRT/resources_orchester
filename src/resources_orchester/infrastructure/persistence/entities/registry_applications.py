
from typing import List

from resources_orchester.infrastructure.persistence.entities.base import Base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, registry, relationship
from sqlalchemy.dialects.postgresql import JSONB

class RegistryApplications(Base):
    __tablename__ = 'registry_applications'

    application_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name_application: Mapped[str] = mapped_column(String(150), nullable=False, unique=True)
    description_application: Mapped[str] = mapped_column(String)
    type_application_id: Mapped[int] = mapped_column(Integer, ForeignKey('type_applications_definition.definition_id'), nullable=False)
    location_application: Mapped[dict[str, any]] = mapped_column(JSONB, default='{}')

    type_application = relationship("ApplicationsTypeDefinition", back_populates="applications")

    dictionaries = relationship("DataRefDict", secondary="link_dict_application")

    def __repr__(self):
        return f"RegistryApplications(name='{self.name_application}')"