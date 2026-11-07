from typing import List

from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications
from resources_orchester.infrastructure.persistence.entities.base import Base
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

class ApplicationsTypeDefinition(Base):
    __tablename__ = 'type_applications_definition'

    definition_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name_definition: Mapped[str] = mapped_column(String(150), nullable=False, unique=True)
    description_application: Mapped[str] = mapped_column(String)

    applications: Mapped[List["RegistryApplications"]] = relationship("RegistryApplications", back_populates="type_application")

    def __repr__(self):
        return f"ApplicationsTypeDefinition(name='{self.name_definition}')"