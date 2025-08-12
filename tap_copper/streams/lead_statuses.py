from tap_copper.streams.abstracts import FullTableStream

class LeadStatuses(FullTableStream):
    tap_stream_id = "lead_statuses"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "root"
    path = "lead_statuses"
