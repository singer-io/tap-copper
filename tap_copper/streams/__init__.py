# tap_copper/streams/__init__.py
from .account import Account
from .users import Users
from .leads import Leads
from .people import People
from .companies import Companies
from .opportunities import Opportunities
from .projects import Projects
from .customer_sources import CustomerSources
from .lead_statuses import LeadStatuses
from .contact_types import ContactTypes
from .loss_reasons import LossReasons
from .pipelines import Pipelines
from .pipeline_stages import PipelineStages
from .tags import Tags
from .custom_field_definitions import CustomFieldDefinitions
from .activities_search import ActivitiesSearch
from .tasks import Tasks


# Parent - child links (using tap_stream_id strings, not classes)
Companies.children = ["people", "opportunities"]
Pipelines.children = ["pipeline_stages"]


# Keep STREAMS as before
STREAMS = {
    "account": Account,
    "users": Users,
    "leads": Leads,
    "people": People,
    "companies": Companies,
    "opportunities": Opportunities,
    "projects": Projects,
    "customer_sources": CustomerSources,
    "lead_statuses": LeadStatuses,
    "contact_types": ContactTypes,
    "loss_reasons": LossReasons,
    "pipelines": Pipelines,
    "pipeline_stages": PipelineStages,
    "tags": Tags,
    "custom_field_definitions": CustomFieldDefinitions,
    "activities_search": ActivitiesSearch,
    "tasks": Tasks,
}
