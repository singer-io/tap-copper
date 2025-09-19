from tap_copper.streams.abstracts import IncrementalStream


class Leads(IncrementalStream):
    """Incremental search over leads (page-number pagination)."""
    tap_stream_id = "leads"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "leads/search"
    data_key = None
    uses_page_number = True