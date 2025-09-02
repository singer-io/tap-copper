from typing import Optional, Dict, Any
from tap_copper.streams.abstracts import FullTableStream
from singer import metrics, write_record


class PipelineStages(FullTableStream):
    tap_stream_id = "pipeline_stages"
    key_properties = ("id",)
    replication_method = "FULL_TABLE"

    http_method = "POST"
    path = "pipeline_stages/search"
    data_key = None
    page_size = 200

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        """Plain search endpoint (no path formatting)."""
        return f"{self.client.base_url}/{self.path}"

    def sync(  # minimal override to inject parent filter; still uses base get_records()
        self,
        state: Dict[str, Any],
        transformer,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> int:
        # Preserve any defaults; do not wipe class-initialized params/body
        self.reset_request()

        body: Dict[str, Any] = {
            "page_size": self.page_size,
            "page_number": 1,
        }
        if parent_obj and "id" in parent_obj:
            body["pipeline_id"] = parent_obj["id"]
        self.update_data_payload(**body)
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed = transformer.transform(record, self.schema, self.metadata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
            return counter.value
