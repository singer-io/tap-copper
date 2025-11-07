
from base import copperBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class copperInterruptedSyncTest(InterruptedSyncTest, copperBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_copper_interrupted_sync_test"

    def streams_to_test(self):
        streams_to_exclude = {"pipeline_stages"}.union(self.get_full_table_streams())
        return self.expected_stream_names().difference(streams_to_exclude)

    def manipulate_state(self):
        return {
            "currently_syncing": "people",
            "bookmarks": {
                "people": {"date_modified": "2025-01-09T00:00:00Z"},
                "tasks": {"date_modified": "2025-01-09T00:00:00Z"},
                "activities_search": {"date_modified": "2025-01-09T00:00:00Z"},
                "companies": {"date_modified": "2025-01-09T00:00:00Z"},
                "leads": {"date_modified": "2025-01-09T00:00:00Z"},
                "opportunities": {"date_modified": "2025-01-09T00:00:00Z"}
            }
        }
