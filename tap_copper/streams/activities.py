from tap_copper.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()


class ActivitiesStream(BaseStream):
    API_METHOD = 'POST'
    TABLE = 'activities'
    KEY_PROPERTIES = ['id']

        
    @property
    def path(self):
        return '/activities/search'