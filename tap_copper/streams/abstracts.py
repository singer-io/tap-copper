"""Base stream abstractions for tap-copper.

Centralizes:
- Pagination loop (list responses and dict+data_key)
- Incremental sync with bookmarking and sorted ASC by replication key
- Parent/child hooks to inject body filters without duplicating sync code
- Safe reset of params/body preserving class defaults
- Type-safe bookmark writes to avoid mixed-type max() errors
"""

from abc import ABC, abstractmethod
from typing import cast
from typing import Any, Dict, Iterator, List, Optional, Tuple

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


class BaseStream(ABC):  # pylint: disable=too-many-instance-attributes
    """Base class for all streams."""

    # Request configuration (override per-stream where needed)
    url_endpoint: str = ""
    path: str = ""
    page_size: int = 200  # sensible default; override per-stream if needed
    next_page_key: str = ""  # if API returns explicit cursor/key in response
    headers: Dict[str, str] = {
        "X-PW-AccessToken": "{{ api_key }}",
        "X-PW-Application": "developer_api",
        "X-PW-UserEmail": "{{ user_email }}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    children: List["BaseStream"] = []
    parent: str = ""
    data_key: str = ""  # if response is dict with records under this key
    parent_bookmark_key: str = ""
    http_method: str = "POST"

    def __init__(self, client: Any = None, catalog: Any = None) -> None:
        """Initialize stream with HTTP client and Singer catalog entry."""
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync: List["BaseStream"] = []
        self.params: Dict[str, Any] = {}
        self.data_payload: Dict[str, Any] = {}
        # Defaults preserved across resets (can be overridden per stream)
        self.default_params: Dict[str, Any] = {}
        self.default_body: Dict[str, Any] = {}


    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream."""

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> List[str]:
        """Replication key(s) for incremental sync."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, ...]:
        """Primary key field(s) for the stream."""


    def is_selected(self) -> bool:
        """Return whether this stream is selected in the catalog."""
        return bool(metadata.get(self.metadata, (), "selected"))

    @abstractmethod
    def sync(
        self,
        state: Dict[str, Any],
        transformer: Transformer,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Perform the replication sync."""


    def _extract_items(self, response: Any) -> List[Dict[str, Any]]:
        """Normalize API responses into a list of records."""
        if isinstance(response, list):
            return response
        if isinstance(response, dict) and self.data_key:
            items = response.get(self.data_key, [])
            return items if isinstance(items, list) else []
        return []

    def _advance_body_page(self) -> None:
        """Increment body page number (when API uses body-based pagination)."""
        next_page = int(self.data_payload.get("page_number", 1)) + 1
        self.update_data_payload(page_number=next_page)

    def reset_request(self) -> None:
        """Reset params/body to known defaults (not empty dicts)."""
        self.params.clear()
        self.params.update(self.default_params)
        self.data_payload.clear()
        self.data_payload.update(self.default_body)

    def get_records(self) -> Iterator[Dict[str, Any]]:
        """
        Generic API interaction + pagination loop.

        Supports:
        - list responses (top-level)
        - dict responses with records under `data_key`
        - either explicit `next_page_key` in response or body-based page_number
        """
        # Some generators set this accidentally; ensure it won't break requests
        self.params.pop("", None)

        while True:
            response = self.client.make_request(
                self.http_method,
                self.url_endpoint,
                self.params,
                self.headers,
                body=self.data_payload,
                path=self.path,
            )

            items = self._extract_items(response)
            yield from items

            # Strategy 1: explicit next_page_key in response
            if self.next_page_key and isinstance(response, dict):
                next_token = response.get(self.next_page_key)
                if not next_token:
                    break
                # If query-string paging is used:
                self.params[self.next_page_key] = next_token
                continue

            # Strategy 2: body-based pagination using page_size/page_number
            if self.page_size and len(items) >= self.page_size:
                self._advance_body_page()
                continue

            break


    def write_schema(self) -> None:
        """Emit schema for this stream."""
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error("OS Error while writing schema for: %s", self.tap_stream_id)
            raise err

    def update_params(self, **kwargs: Any) -> None:
        """Update query parameters for the request."""
        self.params.update(kwargs)

    def update_data_payload(self, **kwargs: Any) -> None:
        """Update JSON body for the request."""
        self.data_payload.update(kwargs)


    def modify_object(
        self,
        record: Dict[str, Any],
        _parent_record: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Hook to modify an item before writing."""
        return record

    def update_parent_filters(self, parent_obj: Optional[Dict[str, Any]]) -> None:
        """
        Hook for child streams to inject parent filters into the request body.
        Example: self.update_data_payload(company_ids=[parent_obj["id"]])
        """
        _ = parent_obj  # placeholder

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        """Build URL endpoint with safe slash handling."""
        _ = parent_obj  # placeholder; override if you need path params
        if self.url_endpoint:
            return self.url_endpoint
        return f"{self.client.base_url}/{str(self.path).lstrip('/')}"


class IncrementalStream(BaseStream):
    """Base class for incremental streams (centralized incremental sync)."""

    @staticmethod
    def _to_int(val: Any) -> int:
        """Best-effort int coercion for bookmarks; returns 0 on failure."""
        try:
            return int(val)
        except (TypeError, ValueError):
            return 0

    def get_bookmark(
        self,
        state: Dict[str, Any],
        stream: str,
        key: Optional[str] = None,
    ) -> int:
        """Read bookmark; default `start_date` is required in config."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(
        self,
        state: Dict[str, Any],
        stream: str,
        key: Optional[str] = None,
        value: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Write bookmark using the max of current and new values (type-safe)."""
        if not (key or self.replication_keys):
            return state
        rk = key or self.replication_keys[0]
        current_raw = get_bookmark(state, stream, rk, self.client.config["start_date"])
        current = self._to_int(current_raw)
        new_val = current
        if value is not None:
            new_val = max(current, self._to_int(value))
        return write_bookmark(state, stream, rk, new_val)

    def _prepare_first_request(
        self,
        *,
        bookmark: Optional[int],
        parent_obj: Optional[Dict[str, Any]],
    ) -> None:
        """Reset params/body and set first-page filters for incremental sync."""
        self.reset_request()

        # Base pagination and sorting on replication key
        self.update_data_payload(
            page_size=self.page_size,
            page_number=1,
            sort_by=self.replication_keys[0] if self.replication_keys else "date_modified",
            sort_direction="asc",
        )
        if bookmark is not None:
            # Copper search endpoints expect minimum_modified_date
            self.update_data_payload(minimum_modified_date=bookmark)

        # Allow child streams to inject parent filters (e.g., company_ids)
        self.update_parent_filters(parent_obj)

        self.url_endpoint = self.get_url_endpoint(parent_obj)

    def sync(
        self,
        state: Dict[str, Any],
        transformer: Transformer,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Default incremental implementation with bookmark advance."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max = bookmark_date

        self._prepare_first_request(bookmark=bookmark_date, parent_obj=parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed = transformer.transform(record, self.schema, self.metadata)

                rk = transformed.get(self.replication_keys[0]) if self.replication_keys else None
                if rk is not None and (bookmark_date is None or rk >= bookmark_date):
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()
                    if current_max is None or rk > current_max:
                        current_max = rk

                    # Propagate to children if any
                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            state = self.write_bookmark(state, self.tap_stream_id, value=current_max)
            return counter.value


class FullTableStream(BaseStream):
    """Base class for full-table streams."""

    replication_keys: List[str] = []

    def sync(
        self,
        state: Dict[str, Any],
        transformer: Transformer,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Default full-table implementation."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        if parent_obj:
            # Allow per-stream usage, but don't force—safe no-op otherwise
            self.update_data_payload(**parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed = transformer.transform(record, self.schema, self.metadata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value


class ParentBaseStream(IncrementalStream):
    """Base class for parent streams with child-aware bookmarks."""

    def get_bookmark(
        self,
        state: Dict[str, Any],
        stream: str,
        key: Optional[str] = None,
    ) -> int:
        """Minimum of parent’s own bookmark and child bookmarks (if selected)."""
        min_parent = super().get_bookmark(state, stream)
        if not self.is_selected():
            min_parent = None

        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            child_bm = super().get_bookmark(state, child.tap_stream_id, key=bookmark_key)
            min_parent = min(min_parent, child_bm) if min_parent is not None else child_bm

        return min_parent

    def write_bookmark(
        self,
        state: Dict[str, Any],
        stream: str,
        key: Optional[str] = None,
        value: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Write parent bookmark and child bookmarks (namespaced)."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)
        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            super().write_bookmark(state, child.tap_stream_id, key=bookmark_key, value=value)
        return state


class ChildBaseStream(IncrementalStream):
    """Base class for child streams."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize child stream and ensure a cached bookmark holder exists."""
        super().__init__(*args, **kwargs)
        self.bookmark_value: Optional[int] = None

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        """
        Default child URL formatter for path-param style endpoints.
        If your child uses a *search* endpoint (body filter only), override this in the stream to:
            return f"{self.client.base_url}/{self.path}"
        """
        if not parent_obj or "id" not in parent_obj:
            return f"{self.client.base_url}/{str(self.path).lstrip('/')}"
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def update_parent_filters(self, parent_obj: Optional[Dict[str, Any]]) -> None:
        """
        Override in child streams that filter by parent via body, e.g.:
            if parent_obj and "id" in parent_obj:
                self.update_data_payload(company_ids=[parent_obj["id"]])
        """
        _ = parent_obj

    def get_bookmark(
        self,
        state: Dict[str, Any],
        stream: str,
        key: Optional[str] = None,
    ) -> int:
        """Singleton/cached bookmark lookups for child streams."""
        if self.bookmark_value is None:
            self.bookmark_value = super().get_bookmark(state, stream)
        return cast(int, self.bookmark_value)
