from typing import Optional, Dict, Any
from tap_copper.streams.abstracts import FullTableStream


class PipelineStages(FullTableStream):
    """Full-table search for pipeline stages (page-number pagination)."""
    tap_stream_id = "pipeline_stages"
    key_properties = ("id",)
    replication_method = "FULL_TABLE"

    http_method = "POST"
    path = "pipeline_stages/search"
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
        # if this ever runs as a child of a pipeline, pass the id
        if parent_obj and "id" in parent_obj:
            base["pipeline_id"] = parent_obj["id"]
        super().update_data_payload(parent_obj=parent_obj, **{**base, **kwargs})
