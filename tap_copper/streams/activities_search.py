from tap_copper.streams.abstracts import IncrementalStream


class ActivitiesSearch(IncrementalStream):
    """Incremental search over activities (page-number pagination)."""
    tap_stream_id = "activities_search"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "activities/search"
    data_key = None  # set to "items" if API wraps results under that key
    page_size = 200

    # Copper /search uses page_number; base will auto-advance when full pages arrive
    uses_page_number = True

    def update_data_payload(self, parent_obj=None, **kwargs):
        base = {
            self.page_number_field: 1,
            self.page_size_field: self.page_size,
            "sort_by": self.replication_keys[0],
            "sort_direction": "asc",
        }
        # pass bookmark in body if set via params by base
        if "updated_since" in self.params and self.params["updated_since"] is not None:
            base["minimum_modified_date"] = self.params["updated_since"]
        super().update_data_payload(parent_obj=parent_obj, **{**base, **kwargs})
