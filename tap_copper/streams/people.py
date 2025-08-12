from tap_copper.streams.abstracts import ChildBaseStream

class People(ChildBaseStream):
    tap_stream_id = "people"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    path = "people/search"
    parent = "companies"
    bookmark_value = None
