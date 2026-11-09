"""
ORM entities. Importing the package registers EVERY mapped class, so relationships declared
by name (e.g. RegistryApplications.type_application -> "ApplicationsTypeDefinition") always
resolve, whichever entity module is imported first.
"""
from resources_orchester.infrastructure.persistence.entities.applications_type_definition import ApplicationsTypeDefinition
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict
from resources_orchester.infrastructure.persistence.entities.link_dict_application import DataDictionaryAppRef
from resources_orchester.infrastructure.persistence.entities.outbox_event import OutboxEvent
from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications
from resources_orchester.infrastructure.persistence.entities.resource_user_access import ResourceUserAccess

__all__ = ["ApplicationsTypeDefinition", "DataRefDict", "DataDictionaryAppRef", "OutboxEvent", "RegistryApplications",
           "ResourceUserAccess"]
