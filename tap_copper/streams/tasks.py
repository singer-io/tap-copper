from tap_copper.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()


class TasksStream(BaseStream):
    API_METHOD = 'POST'
    TABLE = 'tasks'
    KEY_PROPERTIES = ['id']

        
    @property
    def path(self):
        return '/tasks/search'