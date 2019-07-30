from tap_copper.streams.users import UsersStream
from tap_copper.streams.people import PeopleStream
from tap_copper.streams.leads import LeadsStream
from tap_copper.streams.companies import CompaniesStream
from tap_copper.streams.opportunities import OpportunitiesStream
from tap_copper.streams.projects import ProjectsStream
from tap_copper.streams.tasks import TasksStream


AVAILABLE_STREAMS = [
   UsersStream,
   PeopleStream,
   LeadsStream,
   CompaniesStream,
   OpportunitiesStream,
   ProjectsStream,
   TasksStream,
]

__all__ = [
   'UsersStream',
   'PeopleStream',
   'LeadsStream',
   'CompaniesStream',
   'OpportunitiesStream',
   'ProjectsStream',
   'TasksStream',
]
