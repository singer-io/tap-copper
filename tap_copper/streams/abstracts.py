"""Base stream abstractions for tap-copper, aligned with reference taps.

- Support both token and page_number/page_size pagination.
- Provide IncrementalStream, FullTableStream, and ChildBaseStream.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Tuple, Optional, cast

from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metadata,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
)

LOGGER = get_logger()


class BaseStream(ABC):
    """Reference-style base class providing request/pagination scaffolding."""

    # Endpoint / HTTP
    url_endpoint: str = ""
    path: str = ""
    http_method: str = "GET"
    headers: Dict[str, str] = {
        "X-PW-AccessToken": "{{ api_key }}",
        "X-PW-Application": "developer_api",
        "X-PW-UserEmail": "{{ user_email }}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # Pagination / response parsing
    page_size: int = 200
    next_page_key: str = ""  # e.g. "next_token"
    pagination_in: Optional[str] = None  # "params" | "body" | None
    page_number_field: str = "page_number"
    page_size_field: str = "page_size"
    data_key: Optional[str] = None  # None=list response; "items" for dict payloads

    # Parent/child
    children: List[str] = []
    parent: str = ""  # parent tap_stream_id if any
    parent_bookmark_key: str = ""

    def __init__(self, client: Any = None, catalog: Any = None) -> None:
        """Initialize stream with client and catalog-derived schema/metadata."""
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync: List[BaseStream] = []
        self.params: Dict[str, Any] = {}
        self.data_payload: Dict[str, Any] = {}
        # Keep instance attrs <= 7 for pylint.

    # ----- required stream descriptors -------------------------------------------------

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream (may differ from file/class name)."""

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Singer replication method: 'INCREMENTAL' or 'FULL_TABLE'."""

    @property
    @abstractmethod
    def replication_keys(self) -> List[str]:
        """Replication key(s) used for INCREMENTAL syncs."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, ...]:
        """Primary key column(s)."""

    # ----- helpers --------------------------------------------------------------------

    def is_selected(self) -> bool:
        """Return True if the stream is selected in the catalog."""
        return bool(metadata.get(self.metadata, (), "selected"))

    def write_schema(self) -> None:
        """Emit the schema to Singer."""
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error("Failed writing schema for %s: %s", self.tap_stream_id, err)
            raise

    def update_params(self, **kwargs: Any) -> None:
        """Merge query params for the next request."""
        self.params.update(kwargs)

    def update_data_payload(self, **kwargs: Any) -> None:
        """Merge JSON body fields for the next request."""
        self.data_payload.update(kwargs)

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        """Resolve the absolute endpoint URL."""
        del parent_obj  # unused by default
        if self.url_endpoint:
            return self.url_endpoint
        return f"{self.client.base_url}/{str(self.path).lstrip('/')}"

    def modify_object(
        self,
        record: Dict[str, Any],
        _parent_record: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Hook for subclasses to tweak a record before writing."""
        return record

    # ----- pagination -----------------------------------------------------------------

    def update_pagination_key(self, response: Dict[str, Any]) -> Optional[Any]:
        """Extract and store a pagination token if present."""
        if not self.next_page_key:
            return None
        token = response.get(self.next_page_key)
        if not token:
            return None
        if self.pagination_in == "params":
            self.params[self.next_page_key] = token
        elif self.pagination_in == "body":
            self.data_payload[self.next_page_key] = token
        return token

    def _extract_records(self, response: Any) -> List[Dict[str, Any]]:
        """Return a list of record dicts from either list or dict responses."""
        if isinstance(response, list):
            return response
        if isinstance(response, dict):
            if self.data_key is None:
                return []
            raw = response.get(self.data_key, [])
            return raw if isinstance(raw, list) else []
        return []

    def _response_has_more(self, response: Dict[str, Any]) -> Optional[bool]:
        """Try to infer 'has more' from common fields; return None if unknown."""
        # Prefer explicit hints if the API offers them
        for key in ("has_more", "hasMore", "has_next_page", "hasNextPage"):
            if key in response:
                return bool(response.get(key))
        # Some APIs provide a next-page token under known keys
        for key in ("next_page", "nextPage", "next", "nextToken", "next_token"):
            if response.get(key):
                return True
        return None

    def get_records(self) -> Iterator[Dict[str, Any]]:
        """Drive HTTP calls and iterate paginated results."""
        next_page: Any = 1
        while next_page:
            response = self.client.make_request(
                self.http_method,
                self.get_url_endpoint(),
                self.params,
                self.headers,
                body=self.data_payload,
                path=self.path,
            )

            records: List[Dict[str, Any]]
            if isinstance(response, list):
                records = response
                next_page = None
            elif isinstance(response, dict):
                records = self._extract_records(response)

                # 1) Prefer token-based pagination if configured
                next_page = self.update_pagination_key(response)

                # 2) If token wasn't set, prefer explicit end-of-data indicators
                if not next_page and self.page_number_field in self.data_payload:
                    explicit_more = self._response_has_more(response)

                    size_val = self.data_payload.get(self.page_size_field, self.page_size)
                    page_size = int(size_val) if size_val is not None else 0

                    if explicit_more is not None:
                        # Trust the API's explicit signal
                        if explicit_more:
                            current = int(self.data_payload.get(self.page_number_field, 1))
                            self.data_payload[self.page_number_field] = current + 1
                            next_page = True
                        else:
                            next_page = None
                    else:
                        # 3) Fallback heuristic: if the page is shorter than page_size, stop;
                        #    else, assume there might be more.
                        if page_size > 0 and len(records) < page_size:
                            next_page = None
                        else:
                            current = int(self.data_payload.get(self.page_number_field, 1))
                            self.data_payload[self.page_number_field] = current + 1
                            next_page = True
            else:
                records = []
                next_page = None

            yield from records


class IncrementalStream(BaseStream):
    """Reference-style incremental stream with bookmark helpers."""

    def _start_value(self) -> int:
        """Fallback start date when config omits it (keeps legacy jobs running)."""
        raw = self.client.config.get("start_date", 0)
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def get_bookmark(
        self, state: Dict[str, Any], stream: str, key: Optional[str] = None
    ) -> int:
        """Read a bookmark value with a safe default."""
        return get_bookmark(state, stream, key or self.replication_keys[0], self._start_value())

    def write_bookmark(
        self,
        state: Dict[str, Any],
        stream: str,
        key: Optional[str] = None,
        value: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Write a bookmark using max(current, value)."""
        if not (key or self.replication_keys):
            return state
        current = get_bookmark(state, stream, key or self.replication_keys[0], self._start_value())
        new_val = max(int(current or 0), int(value or 0))
        return write_bookmark(state, stream, key or self.replication_keys[0], new_val)

    def sync(
        self,
        state: Dict[str, Any],
        transformer: Transformer,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Default incremental implementation."""
        bookmark = self.get_bookmark(state, self.tap_stream_id)
        current_max = bookmark

        # Opt-in to page_number/page_size if a valid positive page size is set.
        if isinstance(self.page_size, int) and self.page_size > 0:
            self.update_data_payload(
                **{self.page_size_field: self.page_size, self.page_number_field: 1}
            )

        # Be liberal: some endpoints expect query param, others expect body field.
        self.update_params(updated_since=bookmark)
        self.update_data_payload(minimum_modified_date=bookmark)

        # Ensure endpoint is computed (no assignment to avoid unused-var).
        self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed = transformer.transform(record, self.schema, self.metadata)

                rk_name = self.replication_keys[0] if self.replication_keys else None
                record_ts = transformed.get(rk_name) if rk_name else None
                if record_ts is None:
                    continue

                try:
                    record_ts_int = int(record_ts)
                except (TypeError, ValueError):
                    # If schema later moves to ISO strings, skip non-int safely.
                    continue

                if record_ts_int >= int(bookmark):
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()

                    current_max = max(current_max, record_ts_int)

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            self.write_bookmark(state, self.tap_stream_id, value=current_max)
            return counter.value


class FullTableStream(BaseStream):
    """Reference-style full-table stream."""

    replication_keys: List[str] = []

    def sync(
        self,
        state: Dict[str, Any],
        transformer: Transformer,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Default full-table implementation."""
        # Opt-in to page_number/page_size for search-style endpoints (only if valid).
        if isinstance(self.page_size, int) and self.page_size > 0:
            self.update_data_payload(
                **{self.page_size_field: self.page_size, self.page_number_field: 1}
            )

        # Ensure endpoint is computed (no assignment to avoid unused-var).
        self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed = transformer.transform(record, self.schema, self.metadata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value


class ChildBaseStream(IncrementalStream):
    """Small helper so imports like `from ...abstracts import ChildBaseStream` work."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Cache a single bookmark value for children to avoid repeated lookups."""
        super().__init__(*args, **kwargs)
        self.bookmark_value: Optional[int] = None

    def get_bookmark(
        self, state: Dict[str, Any], stream: str, key: Optional[str] = None
    ) -> int:
        """Return a cached bookmark value for this child stream."""
        if self.bookmark_value is None:
            self.bookmark_value = super().get_bookmark(state, stream, key)
        return cast(int, self.bookmark_value)
