from tap_copper.streams.abstracts import FullTableStream

class Tags(FullTableStream):
    tap_stream_id = "tags"
    key_properties = ["name"]
    replication_method = "FULL_TABLE"
    data_key = "root"
    params = {"sort_by": "name"}
    path = "tags"
