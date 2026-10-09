from typing import List

from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications
from resources_orchester.infrastructure.persistence.entities.base import Base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Mapped, registry, relationship

class ApplicationsTypeDefinition(Base):
    __tablename__ = 'type_applications_definition'

    definition_id: Mapped[int]  = Column(Integer, primary_key=True)
    name_definition: Mapped[str] = Column(String(150), nullable=False, unique=True)
    description_application: Mapped[str] = Column(String)

    applications: Mapped[List["RegistryApplications"]] = relationship("RegistryApplications", back_populates="type_application")

    def __repr__(self):
        return f"ApplicationsTypeDefinition(name='{self.name_definition}')"