from tap_copper.streams.abstracts import FullTableStream

class ContactTypes(FullTableStream):
    tap_stream_id = "contact_types"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "root"
    path = "contact_types"
