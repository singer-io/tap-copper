from tap_copper.streams.abstracts import FullTableStream


class Tags(FullTableStream):
    tap_stream_id = "tags"
    key_properties = ("name",)
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "tags"
    data_key = "tags"
