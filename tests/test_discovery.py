"""Test tap discovery mode and metadata."""
from base import copperBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class copperDiscoveryTest(DiscoveryTest, copperBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_copper_discovery_test"

    def streams_to_test(self):
        streams_to_exclude = {"pipeline_stages"}
        return self.expected_stream_names().difference(streams_to_exclude)
