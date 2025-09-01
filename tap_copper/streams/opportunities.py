"""Opportunities (child incremental; filters via body)."""

from typing import Optional, Dict, Any
from tap_copper.streams.abstracts import ChildBaseStream


class Opportunities(ChildBaseStream):
    tap_stream_id = "opportunities"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "opportunities/search"  # body-based search
    data_key = None
    page_size = 200

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        """Plain search endpoint (no path formatting)."""
        return f"{self.client.base_url}/{self.path}"

    def update_parent_filters(self, parent_obj: Optional[Dict[str, Any]]) -> None:
        """Inject company_ids filter from parent object."""
        if parent_obj and "id" in parent_obj:
            self.update_data_payload(company_ids=[parent_obj["id"]])
