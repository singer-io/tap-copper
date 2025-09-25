from tap_copper.streams.account import Account
from tap_copper.streams.activities_search import ActivitiesSearch
from tap_copper.streams.companies import Companies
from tap_copper.streams.contact_types import ContactTypes
from tap_copper.streams.custom_field_definitions import CustomFieldDefinitions
from tap_copper.streams.customer_sources import CustomerSources
from tap_copper.streams.lead_statuses import LeadStatuses
from tap_copper.streams.leads import Leads
from tap_copper.streams.loss_reasons import LossReasons
from tap_copper.streams.opportunities import Opportunities
from tap_copper.streams.people import People
from tap_copper.streams.pipeline_stages import PipelineStages
from tap_copper.streams.pipelines import Pipelines
from tap_copper.streams.projects import Projects
from tap_copper.streams.tags import Tags
from tap_copper.streams.tasks import Tasks
from tap_copper.streams.users import Users

STREAMS = {
    "account": Account,
    "activities_search": ActivitiesSearch,
    "companies": Companies,
    "contact_types": ContactTypes,
    "custom_field_definitions": CustomFieldDefinitions,
    "customer_sources": CustomerSources,
    "lead_statuses": LeadStatuses,
    "leads": Leads,
    "loss_reasons": LossReasons,
    "opportunities": Opportunities,
    "people": People,
    "pipeline_stages": PipelineStages,
    "pipelines": Pipelines,
    "projects": Projects,
    "tags": Tags,
    "tasks": Tasks,
    "users": Users,
}
