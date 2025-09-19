from typing import Optional, Dict, Any
from tap_copper.streams.abstracts import FullTableStream


class PipelineStages(FullTableStream):
    """Full-table search for pipeline stages (page-number pagination)."""
    tap_stream_id = "pipeline_stages"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "POST"
    path = "pipeline_stages/search"
    data_key = None
    uses_page_number = True

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        return f"{self.client.base_url}/{self.path}"
