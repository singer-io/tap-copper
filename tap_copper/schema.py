"""Schema loading and metadata preparation for tap-copper."""

from __future__ import annotations

import json
import os
from typing import Dict, Tuple

import singer
from singer import metadata

from tap_copper.streams import STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """Return absolute path for a file relative to this module."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict[str, dict]:
    """Load shared schema fragments from `schemas/shared` for $ref resolution."""
    refs: Dict[str, dict] = {}
    shared_dir = get_abs_path("schemas/shared")
    if not os.path.exists(shared_dir):
        return refs

    for fname in os.listdir(shared_dir):
        fpath = os.path.join(shared_dir, fname)
        if not (os.path.isfile(fpath) and fname.endswith(".json")):
            continue
        with open(fpath, "r", encoding="utf-8") as handle:
            refs[f"shared/{fname}"] = json.load(handle)
    return refs


def get_schemas() -> Tuple[Dict[str, dict], Dict[str, list]]:
    """Load per-stream schemas, resolve $refs, and prepare Singer metadata."""
    # pylint: disable=too-many-locals
    schemas: Dict[str, dict] = {}
    field_metadata: Dict[str, list] = {}

    refs = load_schema_references()

    for stream_name, stream_cls in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, "r", encoding="utf-8") as file_handle:
            raw_schema = json.load(file_handle)

        # Resolve $ref before building metadata.
        schema = singer.resolve_schema_references(raw_schema, refs)
        schemas[stream_name] = schema

        key_props = getattr(stream_cls, "key_properties")
        repl_keys = getattr(stream_cls, "replication_keys") or []
        repl_method = getattr(stream_cls, "replication_method")

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=key_props,
            valid_replication_keys=repl_keys,
            replication_method=repl_method,
        )
        mdata = metadata.to_map(mdata)

        # Mark replication keys as inclusion=automatic (if present in schema).
        props = schema.get("properties", {})
        for rk in repl_keys:
            if rk in props:
                mdata = metadata.write(
                    mdata, ("properties", rk), "inclusion", "automatic"
                )

        # Annotate parent if declared.
        parent_tap_stream_id = getattr(stream_cls, "parent", None)
        if parent_tap_stream_id:
            mdata = metadata.write(
                mdata, (), "parent-tap-stream-id", parent_tap_stream_id
            )

        field_metadata[stream_name] = metadata.to_list(mdata)

    return schemas, field_metadata
