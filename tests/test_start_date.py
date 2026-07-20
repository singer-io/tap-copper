import unittest

from base import copperBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest



@unittest.skip("Copper API responses for this account do not reliably enforce start_date filtering")
class copperStartDateTest(StartDateTest, copperBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_copper_start_date_test"

    def streams_to_test(self):
        return {"activities_search"}

    @property
    def start_date_1(self):
        return "2026-07-20T02:09:00Z"

    @property
    def start_date_2(self):
        return "2026-07-20T02:09:20Z"
