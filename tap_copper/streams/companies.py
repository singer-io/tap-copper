"""Companies (incremental)."""

from tap_copper.streams.abstracts import IncrementalStream


class Companies(IncrementalStream):
    tap_stream_id = "companies"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "companies/search"
    data_key = None
    page_size = 200
