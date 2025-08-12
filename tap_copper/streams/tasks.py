from tap_copper.streams.abstracts import IncrementalStream

class Tasks(IncrementalStream):
    tap_stream_id = "tasks"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    path = "tasks/search"
