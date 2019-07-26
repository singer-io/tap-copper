from tap_copper.streams.users import UsersStream
from tap_copper.streams.people import PeopleStream


AVAILABLE_STREAMS = [
    UsersStream,
    PeopleStream,
]

__all__ = [
    'UsersStream',
    'PeopleStream'
]
