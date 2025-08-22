"""Sync logic for tap-copper: selection, schema writing, and record syncing.

- Handles parent/child stream wiring.
- Writes schemas (recursively) for selected streams.
- Runs parents, which in turn invoke their children.
"""

from typing import Any, Dict, List, MutableMapping, Optional, Set, Type

import singer

from tap_copper.client import Client
from tap_copper.streams import STREAMS

LOGGER = singer.get_logger()


def update_currently_syncing(
    state: MutableMapping[str, Any],
    stream_name: Optional[str],
) -> None:
    """Update `currently_syncing` in state and write it."""
    if not stream_name and singer.get_currently_syncing(state):
        state.pop("currently_syncing", None)
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def _instantiate_stream(cls: Type[Any], client: Client, cat_stream: Any) -> Any:
    """
    Prefer signature (client, catalog_stream); fallback to no-arg + attach attrs.
    """
    try:
        return cls(client, cat_stream)
    except TypeError:
        inst = cls()
        if hasattr(inst, "configure") and callable(getattr(inst, "configure")):
            inst.configure(client, cat_stream)
        else:
            setattr(inst, "client", client)
            setattr(inst, "catalog_stream", cat_stream)
        return inst


def _build_parent_map() -> Dict[str, str]:
    """Build a reverse map of child_name -> parent_name from STREAMS."""
    parent_map: Dict[str, str] = {}
    for parent_name, parent_cls in STREAMS.items():
        children = getattr(parent_cls, "children", []) or []
        for child_name in children:
            if child_name in STREAMS:
                parent_map[child_name] = parent_name
    return parent_map


def _attach_children(parent_stream: Any, client: Client, catalog: singer.Catalog) -> None:
    """Instantiate children declared on the parent and attach them to `child_to_sync`."""
    parent_stream.child_to_sync = []
    children = getattr(parent_stream, "children", []) or []
    for child_name in children:
        child_cls = STREAMS.get(child_name)
        if not child_cls:
            LOGGER.warning("Child stream '%s' not found in STREAMS mapping", child_name)
            continue

        child_cat = catalog.get_stream(child_name)
        if not child_cat:
            LOGGER.info("Skipping child '%s': not present in catalog", child_name)
            continue

        child_obj = _instantiate_stream(child_cls, client, child_cat)
        parent_stream.child_to_sync.append(child_obj)


def _write_schema_recursive(stream: Any, client: Client, catalog: singer.Catalog) -> None:
    """Write schema for a stream and recursively for its children."""
    if getattr(stream, "is_selected", lambda: False)():
        stream.write_schema()

    _attach_children(stream, client, catalog)
    for child in getattr(stream, "child_to_sync", []):
        _write_schema_recursive(child, client, catalog)


def sync(  # pylint: disable=too-many-locals
    client: Client,
    config: Dict[str, Any],  # pylint: disable=unused-argument
    catalog: singer.Catalog,
    state: MutableMapping[str, Any],
) -> None:
    """Sync selected streams from catalog.

    - Parents will run and invoke their children.
    - If a child is selected and its parent is also selected, skip the child's
      top-level sync to avoid double processing (the parent will invoke it).
    """
    try:
        selected_streams: List[str] = [s.stream for s in catalog.get_selected_streams(state)]
        LOGGER.info("Selected streams: %s", selected_streams)

        last_stream = singer.get_currently_syncing(state)
        LOGGER.info("Last/currently syncing stream: %s", last_stream)

        parent_map: Dict[str, str] = _build_parent_map()
        selected_set: Set[str] = set(selected_streams)

        with singer.Transformer() as transformer:
            for stream_name in selected_streams:
                # If this stream is a child AND its parent is also selected, skip top-level run.
                parent_of_this = parent_map.get(stream_name)
                if parent_of_this and parent_of_this in selected_set:
                    LOGGER.info(
                        "Skipping top-level sync for child '%s' because parent '%s' is selected.",
                        stream_name,
                        parent_of_this,
                    )
                    continue

                stream_cls = STREAMS.get(stream_name)
                if not isinstance(stream_cls, type):
                    raise ValueError(f"Stream class not found for: {stream_name}")

                stream = _instantiate_stream(stream_cls, client, catalog.get_stream(stream_name))

                # Write schemas for this stream + any selected children
                _write_schema_recursive(stream, client, catalog)

                LOGGER.info("START Syncing: %s", stream_name)
                update_currently_syncing(state, stream_name)

                try:
                    total_records = stream.sync(state=state, transformer=transformer)
                except (RuntimeError, AttributeError, ValueError) as sync_err:
                    LOGGER.exception("Failed syncing stream '%s': %s", stream_name, sync_err)
                    update_currently_syncing(state, None)
                    continue

                update_currently_syncing(state, None)
                LOGGER.info("FINISHED Syncing: %s, total_records: %s", stream_name, total_records)

    except (KeyError, AttributeError, ValueError) as top_err:
        LOGGER.exception("Unexpected failure in sync(): %s", top_err)
