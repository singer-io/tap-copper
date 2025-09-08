from typing import Dict, Iterator, Any
from tap_copper.streams.abstracts import FullTableStream


class Account(FullTableStream):
    """Single account object (one-shot GET)."""
    tap_stream_id = "account"
    key_properties = ("id",)
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "account"
    data_key = None

    def get_records(self) -> Iterator[Dict[str, Any]]:
        """Emit the single account object once."""
        resp = self.client.get(
            self.get_url_endpoint(), self.params, self.headers, self.path
        )
        if isinstance(resp, dict):
            yield resp
