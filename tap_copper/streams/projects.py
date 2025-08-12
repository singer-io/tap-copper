from tap_copper.streams.abstracts import IncrementalStream

class Projects(IncrementalStream):
    tap_stream_id = "projects"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    path = "projects/search"
