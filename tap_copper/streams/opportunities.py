from typing import Optional, Dict, Any
from tap_copper.streams.abstracts import ChildBaseStream


class Opportunities(ChildBaseStream):
    """Child search for opportunities; filtered by company (page-number pagination)."""
    tap_stream_id = "opportunities"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    parent = "companies"

    http_method = "POST"
    path = "opportunities/search"
    data_key = None
    page_size = 200

    uses_page_number = True

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        return f"{self.client.base_url}/{self.path}"

    def update_parent_filters(self, parent_obj: Optional[Dict[str, Any]]) -> None:
        base: Dict[str, Any] = {
            self.page_number_field: 1,
            self.page_size_field: self.page_size,
            "sort_by": self.replication_keys[0],
            "sort_direction": "asc",
        }
        if parent_obj and "id" in parent_obj:
            base["company_ids"] = [parent_obj["id"]]
        if "updated_since" in self.params and self.params["updated_since"] is not None:
            base["minimum_modified_date"] = self.params["updated_since"]
        self.update_data_payload(**base)
