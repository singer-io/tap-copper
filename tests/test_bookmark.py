from base import copperBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class copperBookMarkTest(BookmarkTest, copperBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%SZ"
    initial_bookmarks = {
        "bookmarks": {
            "activities_search": {"date_modified": "2022-04-08T06:00:00Z"},
            "companies": {"date_modified": "2022-04-08T06:00:00Z"},
            "opportunities": {"date_modified": "2022-04-08T06:00:00Z"},
            "people": {"date_modified": "2022-04-08T06:00:00Z"},
            "tasks": {"date_modified": "2022-02-01T00:00:00Z"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_copper_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {"pipeline_stages", "projects", "leads"}.union(self.get_full_table_streams())
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""
        new_bookmarks = {
            "activities_search": {"date_modified": "2025-08-08T06:00:00Z"},
            "companies": {"date_modified": "2025-08-08T06:00:00Z"},
            "opportunities": {"date_modified": "2025-09-08T06:00:00Z"},
            "people": {"date_modified": "2025-09-08T06:00:00Z"},
            "tasks": {"date_modified": "2025-08-08T00:00:00Z"},

        }

        return new_bookmarks
