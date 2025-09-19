from typing import Optional, Dict, Any
from tap_copper.streams.abstracts import FullTableStream


class Users(FullTableStream):
    """Full-table search for users (page-number pagination)."""
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "POST"
    path = "users/search"
    data_key = None

    uses_page_number = True

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        return f"{self.client.base_url}/{self.path}"
