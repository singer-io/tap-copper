from tap_copper.streams.abstracts import FullTableStream

class Account(FullTableStream):
    tap_stream_id = "account"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "root"
    path = "account"
