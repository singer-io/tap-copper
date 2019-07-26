from tap_copper.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()  # noqa


class UsersStream(BaseStream):
    API_METHOD = 'POST'
    TABLE = 'users'
    KEY_PROPERTIES = ['id']

        
    @property
    def path(self):
        return '/users/search'