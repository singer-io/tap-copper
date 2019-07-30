from tap_copper.streams.users import UsersStream
from tap_copper.streams.people import PeopleStream
from tap_copper.streams.leads import LeadsStream
from tap_copper.streams.companies import CompaniesStream


AVAILABLE_STREAMS = [
    UsersStream,
    PeopleStream,
    LeadsStream,
    CompaniesStream,
]

__all__ = [
    'UsersStream',
    'PeopleStream',
    'LeadsStream',
    'CompaniesStream',
]
