from tap_copper.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()


class CompaniesStream(BaseStream):
    API_METHOD = 'POST'
    TABLE = 'companies'
    KEY_PROPERTIES = ['id']

        
    @property
    def path(self):
        return '/companies/search'