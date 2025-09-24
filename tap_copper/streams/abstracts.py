"""Base stream abstractions for tap-copper"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List, Iterator  # noqa: F401
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    metadata,
)

LOGGER = get_logger()
DEFAULT_PAGE_SIZE = 100


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

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
        self.child_to_sync = []
        self.params = {}
        self.data_payload = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.
        """

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
        """
        Performs a replication sync for the stream.
        """

    def get_records(self) -> Iterator:
        """Interacts with api client interaction and pagination."""
        self.params["page_size"] = self.client.config.get("page_size", DEFAULT_PAGE_SIZE)

        next_page = 1
        while next_page:
            response = self.client.make_request(
                self.http_method,
                self.get_url_endpoint(),
                self.params,
                self.headers,
                body=self.data_payload,
                path=self.path,
            )

            if isinstance(response, list):
                raw_records = response
                next_page = None
            elif isinstance(response, dict):
                raw_records = response.get(self.data_key, {}) if self.data_key else response
                next_page = response.get(self.next_page_key)
            else:
                raise TypeError("Unexpected response type. Expected dict or list.")

            # when raw_records is a dict, yield the dict itself (not its keys)
            if isinstance(raw_records, dict):
                yield raw_records
            elif isinstance(raw_records, list):
                for item in raw_records:
                    if isinstance(item, dict):
                        yield item

            if next_page:
                self.params[self.next_page_key] = next_page

    def write_schema(self) -> None:
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def update_params(self, **kwargs) -> None:
        """Update params for the stream; include page_size if configured."""
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
        # Remove None values to avoid sending junk
        clean = {k: v for k, v in kwargs.items() if v is not None}
        if clean:
            self.data_payload.update(clean)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Get the URL endpoint for the stream
        """
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> Any:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
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

        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

        # avoid TypeError if types differ
        try:
            value = max(current_bookmark, value)
        except TypeError:
            # If incomparable (e.g., str vs int), prefer the new value to keep moving forward
            value = value if value is not None else current_bookmark

        return write_bookmark(
            state, 
            stream, 
            key or self.replication_keys[0], 
            value
        )

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        bookmark_date = self.get_bookmark(
            state=state,
            stream=self.tap_stream_id
        )
        current_max_bookmark_date = bookmark_date

        # DO NOT add updated_since automatically (Copper search endpoints 422 on this)
        # self.update_params(updated_since=bookmark_date)

        # DO NOT put parent_obj into POST bodies (causes 422)
        # self.update_data_payload(parent_obj=parent_obj)
        self.update_data_payload()  # no-op; keeps body clean
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)

                # Ensure we only transform dict records (cheap guard)
                if not isinstance(record, dict):
                    LOGGER.warning("%s: skipping non-object record (%s)", self.tap_stream_id, type(record).__name__)
                    continue

                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )

                record_bookmark = transformed_record[self.replication_keys[0]]

                # robust compare when types differ
                try:
                    cond = (record_bookmark >= bookmark_date)
                except TypeError:
                    # If incomparable (e.g., int vs str), emit to avoid data loss
                    cond = True

                if cond:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    # robust max when types differ
                    try:
                        current_max_bookmark_date = max(
                            current_max_bookmark_date, record_bookmark
                        )
                    except TypeError:
                        current_max_bookmark_date = record_bookmark

                    for child in self.child_to_sync:
                        child.sync(
                            state=state,
                            transformer=transformer,
                            parent_obj=record,
                        )

            state = self.write_bookmark(
                state=state,
                stream=self.tap_stream_id,
                value=current_max_bookmark_date,
            )
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_keys = []

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        # Never send parent_obj in body
        self.update_data_payload()

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                #  only transform dict records
                if not isinstance(record, dict):
                    LOGGER.warning("%s: skipping non-object record (%s)", self.tap_stream_id, type(record).__name__)
                    continue

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
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

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
        """Write bookmark for parent and propagate to children."""
        if self.is_selected():
            super().write_bookmark(
                state=state,
                stream=stream, 
                key=key, 
                value=value
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
        """Prepare URL endpoint for child streams."""
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> Any:
        """Singleton bookmark value for child streams."""
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(
                state=state, 
                stream=stream, 
                key=key
            )
        return self.bookmark_value
