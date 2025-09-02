from tap_copper.streams.abstracts import IncrementalStream


class ActivitiesSearch(IncrementalStream):
    tap_stream_id = "activities_search"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "activities/search"
    data_key = None
    page_size = 200
