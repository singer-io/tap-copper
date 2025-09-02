"""Sync orchestration for tap-copper."""

from typing import Any, Dict, Iterable, List, Tuple
import singer
from singer import Transformer, metadata

from tap_copper.streams import STREAMS

LOGGER = singer.get_logger()


def _instantiate_stream(cls: Any, client: Any, cat_stream: Any) -> Any:
    """
    Instantiate a stream class with (client, catalog_stream).

    All stream classes must implement __init__(client, catalog_stream).
    This avoids half-initialized objects and makes behavior predictable.
    """
    try:
        return cls(client, cat_stream)
    except TypeError as exc:
        raise TypeError(
            f"{getattr(cls, '__name__', str(cls))} must implement "
            "__init__(client, catalog_stream); got TypeError: {exc}"
        ) from exc


def _is_selected(cat_stream: Any) -> bool:
    """
    Return whether the catalog stream is selected.

    Uses Singer metadata selection semantics.
    """
    mdata = metadata.to_map(cat_stream.metadata)
    return bool(metadata.get(mdata, (), "selected"))


def _selected_catalog_streams(catalog: Any) -> Iterable[Any]:
    """Yield selected catalog streams only."""
    for cs in catalog.get_streams():
        if _is_selected(cs):
            yield cs


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update 'currently_syncing' in state and emit state.

    Pass stream_name to set the field; pass empty string to clear.
    """
    if stream_name:
        singer.set_currently_syncing(state, stream_name)
    else:
        # Clear when done
        cur = singer.get_currently_syncing(state)
        if cur:
            try:
                del state["currently_syncing"]
            except KeyError:
                pass
    singer.write_state(state)


def _write_stream_schema(stream_obj: Any) -> None:
    """Write the schema for the provided stream."""
    try:
        stream_obj.write_schema()
    except OSError as err:
        LOGGER.error("Failed to write schema for %s: %s", stream_obj.tap_stream_id, err)
        raise


def _sync_stream(stream_obj: Any, state: Dict) -> Tuple[str, int]:
    """
    Run sync for a single stream object and return (stream_name, record_count).

    The stream object is expected to implement .sync(state, transformer, parent_obj=None)
    as provided by the base classes in tap_copper.streams.abstracts.
    """
    transformer = Transformer()
    update_currently_syncing(state, stream_obj.tap_stream_id)
    count = stream_obj.sync(state=state, transformer=transformer, parent_obj=None)
    return stream_obj.tap_stream_id, int(count or 0)


def sync(client: Any, catalog: Any, state: Dict, **_kwargs: Any) -> None:
    """
    Orchestrate sync for all selected streams in the catalog.

    Accepts and ignores extra keyword args (e.g., config=...) for backward compatibility.

    Steps:
      - Instantiate each selected stream via the registry
      - Write schema
      - Sync records (incremental or full-table per stream)
      - Maintain 'currently_syncing' in state
    """
    LOGGER.info("Starting sync for selected streams")

    # Build list so logs are stable and we can show a summary
    selected = list(_selected_catalog_streams(catalog))
    if not selected:
        LOGGER.info("No streams selected; nothing to do.")
        return

    # Instantiate and write schemas first (fail fast if constructor invalid)
    instances: List[Any] = []
    for cat_stream in selected:
        tap_stream_id = cat_stream.tap_stream_id
        stream_cls = STREAMS.get(tap_stream_id)
        if stream_cls is None:
            raise KeyError(f"Unknown stream '{tap_stream_id}' not found in STREAMS registry")

        inst = _instantiate_stream(stream_cls, client, cat_stream)
        _write_stream_schema(inst)
        instances.append(inst)

    # Sync each stream and log a short summary line
    for inst in instances:
        name, count = _sync_stream(inst, state)
        LOGGER.info("Synced %-30s records=%d", name, count)

    # Clear currently_syncing when done
    update_currently_syncing(state, "")

    LOGGER.info("Finished sync for %d stream(s).", len(instances))
