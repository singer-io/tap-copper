"""Custom field definitions (full-table)."""

from tap_copper.streams.abstracts import FullTableStream


class CustomFieldDefinitions(FullTableStream):
    tap_stream_id = "custom_field_definitions"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "custom_field_definitions"
    data_key = "custom_field_definitions"
