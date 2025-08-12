from base import copperBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

KNOWN_MISSING_FIELDS = {

}


class copperAllFields(AllFieldsTest, copperBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    @staticmethod
    def name():
        return "tap_tester_copper_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)