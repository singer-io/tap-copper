"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import copperBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class copperAutomaticFields(MinimumSelectionTest, copperBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_copper_automatic_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "pipeline_stages",  # inaccessible
            "custom_field_definitions",  # no records in current account
            "pipelines",  # no records in current account
            "tags",  # no records in current account
        }
        return self.expected_stream_names().difference(streams_to_exclude)
