from tap_copper.streams.users import UsersStream
from tap_copper.streams.people import PeopleStream
from tap_copper.streams.leads import LeadsStream


AVAILABLE_STREAMS = [
    UsersStream,
    PeopleStream,
    LeadsStream,
]

__all__ = [
    'UsersStream',
    'PeopleStream',
    'LeadsStream',
]
