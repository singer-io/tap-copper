"""Base stream abstractions for tap-copper (complete file with date/schema/state normalization)"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List, Iterator
from datetime import datetime, timezone
from copy import deepcopy

from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    write_state,   # used to emit normalized STATE
    metadata,
)

# Generic HTTP→domain exceptions
from tap_copper.exceptions import (
    CopperError,
    CopperNotFoundError,       # 404
    CopperUnauthorizedError,   # 401
    CopperForbiddenError,      # 403 (if defined in your mapping)
)

LOGGER = get_logger()
DEFAULT_PAGE_SIZE = 100


# -----------------------------
# Helpers: time normalization
# -----------------------------
def _to_epoch_seconds(value: Any) -> float:
    """
    Coerce supported timestamp representations into epoch seconds (UTC).
    """
    if value is None:
        raise ValueError("No timestamp value")

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception as exc:
            raise ValueError(f"Unparseable datetime string: {value}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.timestamp()

    raise ValueError(f"Unsupported timestamp type: {type(value)}")


def _to_iso8601_z(value: Any) -> str:
    """Return an ISO-8601 UTC string ('...Z') for a timestamp-like value."""
    ts = _to_epoch_seconds(value)
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class BaseStream(ABC):
    url_endpoint = ""
    path = ""
    page_size = 0
    next_page_key = "page_number"
    headers = {
        "X-PW-AccessToken": "{{ api_key }}",
        "X-PW-Application": "developer_api",
        "X-PW-UserEmail": "{{ user_email }}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    children: List = []
    parent = ""
    data_key = ""
    parent_bookmark_key = ""
    http_method = "POST"
    page_number_field = "page_number"
    page_size_field = "page_size"

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync: List = []
        self.params: Dict[str, Any] = {}
        self.data_payload: Dict[str, Any] = {}

        # Cache which fields look like datetimes (from schema + naming)
        self._date_fields = self._infer_date_fields()

        # Ensure schema advertises date-like fields as string date-time (not numeric)
        self._patch_schema_date_fields()

    # -----------------------------
    # Schema/record/date utilities
    # -----------------------------
    def _infer_date_fields(self) -> set:
        """
        Detect date/datetime fields from the JSON Schema and common naming patterns.
        """
        props = (self.schema or {}).get("properties", {}) or {}
        date_fields: set = set()
        for key, spec in props.items():
            if not isinstance(spec, dict):
                continue
            fmt = spec.get("format")
            if fmt in {"date-time", "date"}:
                date_fields.add(key)
            # Naming heuristics
            if key.endswith("_date") or key.startswith("date_"):
                date_fields.add(key)
        # Common Copper fields (defensive backstop)
        date_fields |= {"date_created", "date_modified", "activity_date"}
        return date_fields

    def _patch_schema_date_fields(self) -> None:
        """
        Rewrite schema so any date-like fields are strings with format date-time.
        Converts number/integer → ["null","string"] + format=date-time.
        Ensures existing string types include format=date-time.
        """
        if not isinstance(self.schema, dict):
            return
        props = self.schema.get("properties")
        if not isinstance(props, dict):
            return

        new_props = deepcopy(props)
        changed = False

        for key in list(new_props.keys()):
            if key not in self._date_fields:
                continue
            spec = new_props.get(key) or {}
            if not isinstance(spec, dict):
                continue

            typ = spec.get("type")
            type_set = set(typ) if isinstance(typ, list) else ({typ} if typ else set())

            # Convert numeric date fields to strings with format=date-time
            if type_set & {"number", "integer"}:
                new_props[key] = {
                    "type": ["null", "string"],
                    "format": "date-time",
                }
                changed = True
            else:
                # Ensure string-based fields carry format=date-time
                if isinstance(typ, list) and "string" in typ:
                    if spec.get("format") != "date-time":
                        spec = dict(spec)
                        spec["format"] = "date-time"
                        new_props[key] = spec
                        changed = True
                elif typ == "string" and spec.get("format") != "date-time":
                    spec = dict(spec)
                    spec["format"] = "date-time"
                    new_props[key] = spec
                    changed = True

        if changed:
            self.schema = dict(self.schema)
            self.schema["properties"] = new_props

    def _normalize_record_dates(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize any recognized date fields on a record to ISO8601Z strings.
        """
        if not isinstance(record, dict):
            return record
        for k in list(record.keys()):
            if k in self._date_fields and record[k] is not None:
                try:
                    record[k] = _to_iso8601_z(record[k])
                except Exception:
                    # Leave as-is if unparseable
                    pass
        return record

    def _normalize_state_bookmarks(self, state: Dict) -> Dict:
        """
        Normalize any date-like bookmark values in STATE to ISO8601Z.
        Emits a normalized STATE immediately if anything changed.
        """
        if not isinstance(state, dict):
            return state
        changed = False
        bookmarks = state.get("bookmarks")
        if not isinstance(bookmarks, dict):
            return state

        def _is_date_key(k: str) -> bool:
            return (
                k in {"date_created", "date_modified", "activity_date"}
                or k.endswith("_date")
                or k.startswith("date_")
            )

        for _, payload in bookmarks.items():
            if isinstance(payload, dict):
                for k, v in list(payload.items()):
                    if v is None:
                        continue
                    if _is_date_key(k):
                        try:
                            iso = _to_iso8601_z(v)
                        except Exception:
                            continue
                        if iso != v:
                            payload[k] = iso
                            changed = True

        if changed:
            write_state(state)
        return state

    # -----------------------------
    # Core stream contract
    # -----------------------------
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
    def replication_keys(self) -> List:
        """Defines the replication key for incremental sync mode of a stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    def is_selected(self):
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Performs a replication sync for the stream."""

    # -----------------------------
    # Network/pagination
    # -----------------------------
    def get_records(self) -> Iterator:
        """Interacts with api client interaction and pagination."""
        self.params["page_size"] = self.client.config.get("page_size", DEFAULT_PAGE_SIZE)

        next_page = 1
        has_yielded = False  # track if we've emitted anything yet

        while next_page:
            try:
                response = self.client.make_request(
                    self.http_method,
                    self.get_url_endpoint(),
                    self.params,
                    self.headers,
                    body=self.data_payload,
                    path=self.path,
                )

            # ---- Generic auth surfacing (NO hardcoding) ----
            except (CopperUnauthorizedError, CopperForbiddenError) as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                msg = (
                    f"{self.tap_stream_id}: unauthorized to access '{self.path}' "
                    f"(HTTP {status or '401/403'}). Check plan/permissions/scopes."
                )
                raise type(e)(msg, getattr(e, "response", None)) from e

            # ---- Optional: endpoint not available but vendor returns 404 ----
            except CopperNotFoundError as e:
                if not has_yielded and (next_page == 1):
                    raise CopperUnauthorizedError(
                        f"{self.tap_stream_id}: endpoint '{self.path}' is not accessible for this account "
                        f"(received 404). This typically means the feature/endpoint isn't enabled "
                        f"or you lack permission."
                    ) from e
                raise

            if isinstance(response, list):
                raw_records = response
                next_page = None
            elif isinstance(response, dict):
                raw_records = response.get(self.data_key, {}) if self.data_key else response
                next_page = response.get(self.next_page_key)
            else:
                raise TypeError("Unexpected response type. Expected dict or list.")

            if isinstance(raw_records, dict):
                has_yielded = True
                yield raw_records
            elif isinstance(raw_records, list):
                for item in raw_records:
                    if isinstance(item, dict):
                        has_yielded = True
                        yield item

            if next_page:
                self.params[self.next_page_key] = next_page

    def write_schema(self) -> None:
        try:
            # schema already patched to align date-like fields with string/ date-time
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError:
            LOGGER.error("OS Error while writing schema for: %s", self.tap_stream_id)
            raise

    def update_params(self, **kwargs) -> None:
        if isinstance(self.page_size, int) and self.page_size > 0:
            self.params.setdefault("page_size", self.page_size)
        self.params.update(kwargs)

    def update_data_payload(self, **kwargs) -> None:
        """
        Update JSON body for the stream.
        IMPORTANT: Never send parent_obj in body (Copper returns 422 on unknown attrs).
        """
        if "parent_obj" in kwargs:
            kwargs.pop("parent_obj", None)
        clean = {k: v for k, v in kwargs.items() if v is not None}
        if clean:
            self.data_payload.update(clean)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Hook for per-record manipulation.
        Default behavior: normalize date/datetime fields to ISO8601Z.
        """
        return self._normalize_record_dates(record)

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> Any:
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(
        self,
        state: dict,
        stream: str,
        key: Any = None,
        value: Any = None,
    ) -> Dict:
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

        try:
            value = max(current_bookmark, value)
        except TypeError:
            value = value if value is not None else current_bookmark

        iso_value = _to_iso8601_z(value)

        return write_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            iso_value
        )

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        # Normalize any numeric bookmarks in incoming state
        state = self._normalize_state_bookmarks(state)

        raw_bookmark = self.get_bookmark(
            state=state,
            stream=self.tap_stream_id
        )

        # REMOVED: try/except ValueError fallback (fail-fast like other taps)
        bookmark_sec = _to_epoch_seconds(raw_bookmark)

        current_max_sec = bookmark_sec

        self.update_data_payload()  # keep body clean
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)

                if not isinstance(record, dict):
                    LOGGER.warning(
                        "%s: skipping non-object record (%s)",
                        self.tap_stream_id,
                        type(record).__name__,
                    )
                    continue

                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )

                record_bookmark = transformed_record.get(self.replication_keys[0])
                if record_bookmark is None:
                    continue

                # REMOVED: try/except ValueError fallback (fail-fast like other taps)
                rec_sec = _to_epoch_seconds(record_bookmark)

                if rec_sec >= bookmark_sec:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    if rec_sec > current_max_sec:
                        current_max_sec = rec_sec

                    for child in self.child_to_sync:
                        child.sync(
                            state=state,
                            transformer=transformer,
                            parent_obj=record,
                        )

            # This writes the bookmark as ISO8601Z
            state = self.write_bookmark(
                state=state,
                stream=self.tap_stream_id,
                value=current_max_sec,
            )
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for Full Table Stream."""

    replication_keys = []

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        # Normalize any numeric bookmarks in incoming state
        state = self._normalize_state_bookmarks(state)

        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self.update_data_payload()

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                if not isinstance(record, dict):
                    LOGGER.warning(
                        "%s: skipping non-object record (%s)",
                        self.tap_stream_id,
                        type(record).__name__,
                    )
                    continue

                record = self.modify_object(record, parent_obj)

                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(
                        state=state,
                        transformer=transformer,
                        parent_obj=record,
                    )

            return counter.value


class ParentBaseStream(IncrementalStream):
    """Base Class for Parent Stream."""

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> Any:
        min_parent_bookmark = (
            super().get_bookmark(state=state, stream=stream)
            if self.is_selected()
            else None
        )
        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            child_bookmark = super().get_bookmark(
                state=state,
                stream=child.tap_stream_id,
                key=bookmark_key
            )
            min_parent_bookmark = (
                min(min_parent_bookmark, child_bookmark)
                if min_parent_bookmark
                else child_bookmark
            )
        return min_parent_bookmark

    def write_bookmark(
        self,
        state: Dict,
        stream: str,
        key: Any = None,
        value: Any = None,
    ) -> Dict:
        if self.is_selected():
            super().write_bookmark(
                state=state,
                stream=stream,
                key=key,
                value=value,
            )
        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            super().write_bookmark(
                state=state,
                stream=child.tap_stream_id,
                key=bookmark_key,
                value=value,
            )
        return state


class ChildBaseStream(IncrementalStream):
    """Base Class for Child Stream."""

    def __init__(self, client=None, catalog=None) -> None:
        super().__init__(client, catalog)
        self.bookmark_value = None

    def get_url_endpoint(self, parent_obj=None):
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> Any:
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(
                state=state,
                stream=stream,
                key=key
            )
        return self.bookmark_value
