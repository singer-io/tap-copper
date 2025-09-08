"""base classes for tap-copper streams."""

from __future__ import annotations

from abc import ABC, abstractmethod
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


class BaseStream(ABC):
    """Minimal base stream matching amazon/msgraph style."""

    url_endpoint: str = ""
    path: str = ""
    page_size: int = 200
    next_page_key: Optional[str] = None  # e.g., "nextToken", "nextPage"
    pagination_in: Optional[str] = None  # "params" | "body" | None
    data_key: Optional[str] = None
    http_method: str = "POST"

    # Auth is added by Client; keep these generic here.
    headers: Dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    # Relationships (optional; used by orchestration if needed).
    children: List[str] = []
    parent: str = ""
    parent_bookmark_key: str = ""

    def __init__(self, client: Any = None, catalog: Any = None) -> None:
        """Initialize the stream with client and catalog context."""
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync: List[BaseStream] = []
        self.params: Dict[str, Any] = {}
        self.data_payload: Dict[str, Any] = {}

    # ---- required stream attributes ----

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream."""

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Replication mode: 'INCREMENTAL' or 'FULL_TABLE'."""

    @property
    @abstractmethod
    def replication_keys(self) -> List[str]:
        """Replication keys (first one is used as the bookmark key)."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, ...]:
        """Primary key columns."""

    # ---- helpers ----

    def is_selected(self) -> bool:
        """Return True if this stream is selected."""
        return bool(metadata.get(self.metadata, (), "selected"))

    def write_schema(self) -> None:
        """Emit the Singer schema for this stream."""
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for %s: %s", self.tap_stream_id, err
            )
            raise

    def update_params(self, **kwargs) -> None:
        """Merge query parameters for the next request(s)."""
        self.params.update(kwargs)

    def update_data_payload(self, **kwargs) -> None:
        """Merge JSON body fields for the next request(s)."""
        self.data_payload.update(kwargs)

    def modify_object(
        self,
        record: Dict[str, Any],
        parent_record: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Hook to modify a record prior to write_record."""
        _ = parent_record
        return record

    def get_url_endpoint(self, parent_obj: Optional[Dict[str, Any]] = None) -> str:
        """Return fully qualified endpoint (defaults to base_url + path)."""
        _ = parent_obj
        return self.url_endpoint or f"{self.client.base_url}/{str(self.path).lstrip('/')}"

    @staticmethod
    def get_dot_path_value(record: dict, dotted_path: str, default=None):
        """Get nested value using dotted path ('a.b.c')."""
        value = record
        for key in dotted_path.split("."):
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def _extract_records_and_next(
        self,
        response: Any,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Normalize response into (records, next_token)."""
        next_token = None

        if isinstance(response, list):
            return response, None

        if not isinstance(response, dict):
            return [], None

        if self.data_key is None:
            records = response.get("items", [])
            if not isinstance(records, list):
                records = []
        else:
            records = response.get(self.data_key, [])
            if not isinstance(records, list):
                records = []

        if self.next_page_key:
            raw_next = response.get(self.next_page_key)
            next_token = raw_next if raw_next else None

        return records, next_token

    def _apply_next_token(self, next_token: Optional[str]) -> None:
        """Attach the next-page token to params/body according to pagination_in."""
        if not (self.next_page_key and next_token):
            return
        if self.pagination_in == "params":
            self.params[self.next_page_key] = next_token
        elif self.pagination_in == "body":
            self.data_payload[self.next_page_key] = next_token

    def get_records(self) -> Iterator[Dict[str, Any]]:
        """Iterate API pages and yield records."""
        next_token: Optional[str] = None
        first = True

        while first or next_token:
            first = False
            response = self.client.make_request(
                self.http_method,
                self.get_url_endpoint(),
                self.params,
                self.headers,
                body=self.data_payload if self.http_method.upper() == "POST" else None,
                path=self.path,
            )
            records, next_token = self._extract_records_and_next(response)
            yield from records
            self._apply_next_token(next_token)


class IncrementalStream(BaseStream):
    """Base class for incremental streams (start_date optional)."""

    def _default_start(self) -> int:
        """Return config['start_date'] if present, otherwise 0 (epoch)."""
        try:
            return int(self.client.config.get("start_date", 0))
        except (TypeError, ValueError):
            return 0

    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """Read the bookmark value with a safe default."""
        return get_bookmark(state, stream, key or self.replication_keys[0], self._default_start())

    def write_bookmark(
        self,
        state: dict,
        stream: str,
        key: Any = None,
        value: Any = None,
    ) -> Dict:
        """Write the bookmark as max(current, value)."""
        if not (key or self.replication_keys):
            return state
        rk = key or self.replication_keys[0]
        current = get_bookmark(state, stream, rk, self._default_start())
        final = max(current, value) if value is not None else current
        return write_bookmark(state, stream, rk, final)

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Run an incremental sync and update the bookmark."""
        bookmark = self.get_bookmark(state, self.tap_stream_id)
        current_max = bookmark

        self.update_params(updated_since=bookmark)
        self.update_data_payload(**(parent_obj or {}))
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed = transformer.transform(record, self.schema, self.metadata)

                rk_path = self.replication_keys[0]
                record_ts = (
                    transformed.get(rk_path)
                    if "." not in rk_path
                    else self.get_dot_path_value(transformed, rk_path)
                )
                if record_ts is None:
                    continue

                if record_ts >= bookmark and self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

                current_max = max(current_max, record_ts)

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            self.write_bookmark(state, self.tap_stream_id, value=current_max)
            return counter.value


class FullTableStream(BaseStream):
    """Base class for full-table streams."""

    replication_keys: List[str] = []

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Run a full-table sync."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self.update_data_payload(**(parent_obj or {}))

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed = transformer.transform(record, self.schema, self.metadata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value
