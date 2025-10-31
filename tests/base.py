import os

from tap_tester.base_suite_tests.base_case import BaseCase


class copperBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2019-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-copper"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.copper"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "account": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "activities_search": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 30
            },
            "companies": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 5
            },
            "contact_types": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "custom_field_definitions": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "customer_sources": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "lead_statuses": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "leads": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1
            },
            "loss_reasons": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "opportunities": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 5
            },
            "people": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 4
            },
            "pipeline_stages": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "pipelines": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "projects": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1
            },
            "tags": {
                cls.PRIMARY_KEYS: {"name"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "tasks": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 5
            },
            "users": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            }
        }

    @staticmethod
    def get_credentials():
        """Get the credentials for the Copper API.
        Creds Mapping:
            api_key:        COPPER_API_KEY --> used in X-PW-AccessToken header
            user_email:    COPPER_USER_EMAIL --> used in X-PW-UserEmail header

        Returns:
            dict: A dictionary containing the API credentials.
        """

        credentials_dict = {}
        creds = {
            'api_key':    'COPPER_API_KEY',     # X-PW-AccessToken
            'user_email': 'COPPER_USER_EMAIL',  # X-PW-UserEmail
            # add if your client uses base_url/subdomain:
            # 'base_url':   'COPPER_BASE_URL',
            # 'subdomain':  'COPPER_SUBDOMAIN',
        }

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])
        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return {
            "start_date": self.start_date
        }

    def get_full_table_streams(self):
        """ Function to return the full table streams from the class metadata
        """
        to_return = set()
        for stream_name, data in self.expected_metadata().items():
            replication_method = data[self.REPLICATION_METHOD]
            if replication_method == "FULL_TABLE":
                to_return.add(stream_name)

        return to_return
