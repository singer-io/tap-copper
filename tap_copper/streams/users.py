from tap_copper.streams.abstracts import FullTableStream


class Users(FullTableStream):
    tap_stream_id = "users"
    key_properties = ("id",)
    replication_method = "FULL_TABLE"

    http_method = "POST"
    path = "users/search"
    data_key = None
    page_size = 200
