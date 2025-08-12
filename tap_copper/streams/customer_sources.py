from tap_copper.streams.abstracts import FullTableStream

class CustomerSources(FullTableStream):
    tap_stream_id = "customer_sources"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "root"
    path = "customer_sources"
