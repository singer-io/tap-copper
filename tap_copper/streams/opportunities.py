from tap_copper.streams.abstracts import ChildBaseStream

class Opportunities(ChildBaseStream):
    tap_stream_id = "opportunities"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    path = "opportunities/search"
    parent = "companies"
    bookmark_value = None
