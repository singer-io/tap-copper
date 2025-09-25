from typing import Optional, Dict, Any, List, Iterator
from tap_copper.streams.abstracts import ChildBaseStream, DEFAULT_PAGE_SIZE


class People(ChildBaseStream):
    """Search People (flat endpoint) with page-number pagination."""
    tap_stream_id = "people"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    parent = ""

    http_method = "POST"
    path = "people/search"
    data_key = None
    uses_page_number = True

    def __init__(self, client=None, catalog=None) -> None:
        super().__init__(client, catalog)
        pg_size = self.client.config.get("page_size", DEFAULT_PAGE_SIZE)
        self.update_data_payload(page_number=1, page_size=pg_size)

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        return f"{self.client.base_url}/{self.path}"

    def get_records(self) -> Iterator[Dict[str, Any]]:
        """Local pagination loop for /people/search (root-level array)."""
        page_size = self.client.config.get("page_size", DEFAULT_PAGE_SIZE)
        page = 1

        while True:
            params = dict(self.params)
            body = dict(self.data_payload)
            body["page_number"] = page
            body["page_size"] = page_size

            resp = self.client.make_request(
                self.http_method,
                self.get_url_endpoint(),
                params,
                self.headers,
                body=body,
                path=self.path,
            )

            # Normalize to list of dicts
            if isinstance(resp, list):
                items: List[Dict[str, Any]] = [r for r in resp if isinstance(r, dict)]
            elif isinstance(resp, dict):
                if self.data_key and isinstance(resp.get(self.data_key), list):
                    items = [r for r in resp.get(self.data_key, []) if isinstance(r, dict)]
                else:
                    items = [resp]
            else:
                items = []

            for rec in items:
                if isinstance(rec, dict):
                    yield rec

            if len(items) < page_size:
                break

            page += 1
