"""
Sync orchestration for tap-copper.

This module coordinates:
- stream selection from the catalog
- schema emission
- per-stream sync execution
- state updates (currently_syncing, bookmarks)

Pattern aligned with reference taps.
"""

from typing import Any, Dict, List
import singer
from singer import Transformer, metadata
from tap_copper.streams import STREAMS

LOGGER = singer.get_logger()


def _is_selected(cat_stream: Any) -> bool:
    """Return whether the catalog stream is selected."""
    mdata = metadata.to_map(cat_stream.metadata)
    return bool(metadata.get(mdata, (), "selected"))


def update_currently_syncing(state: Dict[str, Any], stream_name: str | None) -> None:
    """Update `currently_syncing` in state and emit the state."""
    if not stream_name and singer.get_currently_syncing(state):
        state.pop("currently_syncing", None)
    else:
        singer.set_currently_syncing(state, stream_name or "")
    singer.write_state(state)


def _instantiate_stream(stream_name: str, client: Any, catalog: Any) -> Any:
    """Instantiate a stream class with (client, catalog_stream) per registry."""
    stream_cls = STREAMS[stream_name]
    return stream_cls(client, catalog.get_stream(stream_name))


def _write_schema_recursive(
    stream_obj: Any,
    client: Any,
    catalog: Any,
    selected_names: List[str],
) -> None:
    """Write schema for the stream and its children; attach selected children."""
    if stream_obj.is_selected():
        stream_obj.write_schema()

    for child_name in getattr(stream_obj, "children", []):
        child_obj = _instantiate_stream(child_name, client, catalog)
        _write_schema_recursive(child_obj, client, catalog, selected_names)
        if child_name in selected_names:
            stream_obj.child_to_sync.append(child_obj)


def sync(client: Any, catalog: Any, state: Dict[str, Any], **_kwargs: Any) -> None:
    """Sync all selected streams using parent-first orchestration."""
    # 1) Gather selected stream names.
    selected_names: List[str] = [
        cs.stream for cs in catalog.get_streams() if _is_selected(cs)
    ]
    LOGGER.info("selected_streams: %s", selected_names)

    # 2) Ensure parents of any selected children are included.
    #    (We append missing parents; children are driven by parents later.)
    i = 0
    while i < len(selected_names):
        name = selected_names[i]
        parent = getattr(STREAMS[name], "parent", None)
        if parent and parent not in selected_names:
            selected_names.append(parent)
        i += 1

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: %s", last_stream)

    # 3) Sync each top-level (non-child) stream. Children are driven via child_to_sync.
    with Transformer() as transformer:
        for stream_name in selected_names:
            if getattr(STREAMS[stream_name], "parent", None):
                # Skip child here; its parent will drive it.
                continue

            stream_obj = _instantiate_stream(stream_name, client, catalog)
            _write_schema_recursive(stream_obj, client, catalog, selected_names)

            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)
            total_records = int(stream_obj.sync(state=state, transformer=transformer) or 0)
            update_currently_syncing(state, None)
            LOGGER.info("FINISHED Syncing: %s, total_records: %d", stream_name, total_records)
