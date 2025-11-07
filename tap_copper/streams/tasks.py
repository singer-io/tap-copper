from tap_copper.streams.abstracts import IncrementalStream


class Tasks(IncrementalStream):
    """Incremental search over tasks (page-number pagination)."""
    tap_stream_id = "tasks"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "tasks/search"
    data_key = None

    uses_page_number = True
