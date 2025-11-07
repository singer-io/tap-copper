from tap_copper.streams.abstracts import FullTableStream


class LeadStatuses(FullTableStream):
    tap_stream_id = "lead_statuses"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "lead_statuses"
    data_key = "lead_statuses"
