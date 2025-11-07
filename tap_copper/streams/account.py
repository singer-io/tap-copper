from tap_copper.streams.abstracts import FullTableStream


class Account(FullTableStream):
    """Single account object (one-shot GET)."""
    tap_stream_id = "account"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "account"
    data_key = None
