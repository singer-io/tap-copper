from tap_copper.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()


class ProjectsStream(BaseStream):
    API_METHOD = 'POST'
    TABLE = 'projects'
    KEY_PROPERTIES = ['id']

        
    @property
    def path(self):
        return '/projects/search'