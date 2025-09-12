from typing import Dict, Iterator, Any
from tap_copper.streams.abstracts import FullTableStream

class Account(FullTableStream):
    """Single account object (one-shot GET)."""
    tap_stream_id = "account"
    key_properties = []
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "account"
    data_key = None

    def get_records(self) -> Iterator[Dict[str, Any]]:
        url = self.get_url_endpoint()
        resp = self.client.make_request(
            "GET",
            url,
            params=getattr(self, "params", None),
            headers=getattr(self, "headers", None),
        )
        if isinstance(resp, dict):
            yield resp
