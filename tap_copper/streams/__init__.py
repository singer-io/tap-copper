"""Stream registry with module-based imports and parent–child links."""

from tap_copper.streams.account import Account
from tap_copper.streams.users import Users
from tap_copper.streams.leads import Leads
from tap_copper.streams.people import People
from tap_copper.streams.companies import Companies
from tap_copper.streams.opportunities import Opportunities
from tap_copper.streams.projects import Projects
from tap_copper.streams.customer_sources import CustomerSources
from tap_copper.streams.lead_statuses import LeadStatuses
from tap_copper.streams.contact_types import ContactTypes
from tap_copper.streams.loss_reasons import LossReasons
from tap_copper.streams.pipelines import Pipelines
from tap_copper.streams.pipeline_stages import PipelineStages
from tap_copper.streams.tags import Tags
from tap_copper.streams.custom_field_definitions import CustomFieldDefinitions
from tap_copper.streams.activities_search import ActivitiesSearch
from tap_copper.streams.tasks import Tasks

# Parent–child links (using tap_stream_id strings)
Companies.children = ["people", "opportunities"]
Pipelines.children = ["pipeline_stages"]

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
