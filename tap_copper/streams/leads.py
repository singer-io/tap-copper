from tap_copper.streams.abstracts import IncrementalStream


class Leads(IncrementalStream):
    """Incremental search over leads (page-number pagination)."""
    tap_stream_id = "leads"
    key_properties = ("id",)
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "leads/search"
    data_key = None
    page_size = 200

    uses_page_number = True

    def update_data_payload(self, parent_obj=None, **kwargs):
        base = {
            self.page_number_field: 1,
            self.page_size_field: self.page_size,
            "sort_by": self.replication_keys[0],
            "sort_direction": "asc",
        }
        if "updated_since" in self.params and self.params["updated_since"] is not None:
            base["minimum_modified_date"] = self.params["updated_since"]
        super().update_data_payload(parent_obj=parent_obj, **{**base, **kwargs})
