from tap_copper.streams.abstracts import IncrementalStream

class Leads(IncrementalStream):
    tap_stream_id = "leads"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    path = "leads/search"
