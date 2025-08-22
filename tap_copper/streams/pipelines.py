from tap_copper.streams.abstracts import FullTableStream

class Pipelines(FullTableStream):
    tap_stream_id = "pipelines"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "pipelines"
    data_key = "pipelines"
