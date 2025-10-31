from base import copperBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest



class copperStartDateTest(StartDateTest, copperBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_copper_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {"pipeline_stages", "leads", "projects"}.union(self.get_full_table_streams())
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2024-08-07T00:00:00Z"

    @property
    def start_date_2(self):
        return "2025-08-09T00:00:00Z"
