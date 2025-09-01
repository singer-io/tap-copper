"""Projects (incremental)."""

from tap_copper.streams.abstracts import IncrementalStream


class Projects(IncrementalStream):
    tap_stream_id = "projects"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "projects/search"
    data_key = None
    page_size = 200
