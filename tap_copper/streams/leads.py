"""Leads (incremental)."""

from tap_copper.streams.abstracts import IncrementalStream


class Leads(IncrementalStream):
    tap_stream_id = "leads"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "leads/search"
    data_key = None
    page_size = 200
