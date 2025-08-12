from tap_copper.streams.abstracts import IncrementalStream

class ActivitiesSearch(IncrementalStream):
    tap_stream_id = "activities_search"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    path = "activities/search"
