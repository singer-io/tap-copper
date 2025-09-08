from typing import Optional, Dict, Any
from tap_copper.streams.abstracts import FullTableStream


class Users(FullTableStream):
    """Full-table search for users (page-number pagination)."""
    tap_stream_id = "users"
    key_properties = ("id",)
    replication_method = "FULL_TABLE"

    http_method = "POST"
    path = "users/search"
    data_key = None
    page_size = 200

    uses_page_number = True

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        return f"{self.client.base_url}/{self.path}"

    def update_data_payload(self, parent_obj: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        base: Dict[str, Any] = {
            self.page_number_field: 1,
            self.page_size_field: self.page_size,
        }
        super().update_data_payload(parent_obj=parent_obj, **{**base, **kwargs})
