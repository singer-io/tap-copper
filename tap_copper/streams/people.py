from typing import Optional, Dict, Any
from tap_copper.streams.abstracts import ChildBaseStream


class People(ChildBaseStream):
    """Child search for people; filtered by company (page-number pagination)."""
    tap_stream_id = "people"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    # declare parent so sync can attach children to the Companies stream
    parent = "companies"

    http_method = "POST"
    path = "people/search"
    data_key = None

    uses_page_number = True

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        return f"{self.client.base_url}/{self.path}"
