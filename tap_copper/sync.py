"""
Sync logic for tap-copper. Handles orchestration of stream syncs and schema writes.
"""

from typing import Dict
import singer
from singer.transform import UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING
from tap_copper.streams import STREAMS
from tap_copper.client import Client
from tap_copper.exceptions import CopperUnauthorizedError

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update currently_syncing in state and write it.
    If stream_name is falsy, clears currently_syncing.
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_schema(stream, client, streams_to_sync, catalog) -> None:
    """
    Write schema for stream and its children; attach selected children for sync.
    """
    if stream.is_selected():
        stream.write_schema()

    for child in stream.children:
        child_obj = STREAMS[child](client, catalog.get_stream(child))
        write_schema(child_obj, client, streams_to_sync, catalog)
        if child in streams_to_sync:
            stream.child_to_sync.append(child_obj)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:
    """
    Sync selected streams from catalog.
    """
    if not catalog or not catalog.get_selected_streams(state):
        LOGGER.warning("No catalog provided or no streams selected. Skipping sync.")
        return

    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.stream)
    LOGGER.info("selected_streams: %s", streams_to_sync)

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: %s", last_stream)

    with singer.Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
        for stream_name in streams_to_sync[:]:
            stream = STREAMS[stream_name](client, catalog.get_stream(stream_name))

            # Ensure parents will be synced first if selected children exist
            if stream.parent:
                if stream.parent not in streams_to_sync:
                    streams_to_sync.append(stream.parent)
                continue

            # Write schemas and attach children that are selected
            write_schema(stream, client, streams_to_sync, catalog)

            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)

            try:
                total_records = stream.sync(state=state, transformer=transformer)
            except CopperUnauthorizedError as e:
                msg = f"Unauthorized stream: {stream_name}. Check API credentials or permissions."
                LOGGER.error(msg)
                raise Exception(msg) from e

            update_currently_syncing(state, None)
            LOGGER.info("FINISHED Syncing: %s, total_records: %s", stream_name, total_records)
