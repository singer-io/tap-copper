"""Schema loading and metadata preparation for tap-copper."""

import json
import os
from typing import Any, Dict, List, Tuple, Optional

import singer
from singer import metadata

from tap_copper.streams import STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """Return absolute path for a file relative to this module."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict[str, Any]:
    """Load shared schema fragments from `schemas/shared` for $ref resolution.
    Only loads *.json files and skips invalid JSON, logging warnings.
    """
    shared_schema_path = get_abs_path("schemas/shared")
    refs: Dict[str, Any] = {}

    if not os.path.exists(shared_schema_path):
        return refs

    for fname in os.listdir(shared_schema_path):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(shared_schema_path, fname)
        if os.path.isfile(fpath):
            try:
                with open(fpath, "r", encoding="utf-8") as fh:
                    refs[f"shared/{fname}"] = json.load(fh)
            except Exception as e:
                LOGGER.warning(f"Failed to load shared schema '{fname}': {e}")

    return refs


def _cls_attr(cls: Any, name: str, default: Any) -> Any:
    """Safely get a class attribute; if it's a descriptor (@property), return default."""
    val = getattr(cls, name, default)
    return default if isinstance(val, property) else val


def _as_list(value: Any, context: str = "") -> List[Any]:
    """Normalize value to a list. Warn if unexpected type."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if isinstance(value, str):
        return [value]
    LOGGER.warning(f"Unexpected type for {context}: {type(value).__name__}. Returning [].")
    return []


def get_schemas() -> Tuple[Dict[str, Any], Dict[str, List[Any]]]:
    """
    Load per-stream schemas and prepare Singer metadata.

    Returns:
        (schemas, field_metadata)
        - schemas: {stream_name: resolved JSON schema}
        - field_metadata: {stream_name: metadata list}
    """
    schemas: Dict[str, Any] = {}
    field_metadata: Dict[str, List[Any]] = {}
    refs = load_schema_references()

    for stream_name, stream_cls in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        if not os.path.exists(schema_path):
            LOGGER.warning(f"Schema file not found: {schema_path}")
            continue

        try:
            with open(schema_path, "r", encoding="utf-8") as file:
                schema = json.load(file)
        except Exception as e:
            LOGGER.warning(f"Failed to load schema for stream '{stream_name}': {e}")
            continue

        # Resolve $ref before building metadata
        try:
            schema = singer.resolve_schema_references(schema, refs)
        except Exception as e:
            LOGGER.warning(f"Failed to resolve $ref for stream '{stream_name}': {e}")
            continue

        # Ensure "properties" exists and is a dict
        if "properties" not in schema or not isinstance(schema["properties"], dict):
            LOGGER.warning(
                f"Schema for {stream_name} missing 'properties'; initializing empty dict."
            )
            schema["properties"] = {}

        schemas[stream_name] = schema

        # Read attributes from CLASS (no instantiation)
        key_props = _as_list(_cls_attr(stream_cls, "key_properties", []), f"{stream_name}.key_properties")
        repl_keys = _as_list(_cls_attr(stream_cls, "replication_keys", []), f"{stream_name}.replication_keys")
        repl_method = _cls_attr(stream_cls, "replication_method", "FULL_TABLE")

        stream_metadata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=key_props,
            valid_replication_keys=repl_keys,
            replication_method=repl_method,
        )
        stream_metadata_map = metadata.to_map(stream_metadata)

        # Mark replication keys as inclusion=automatic if present in schema
        for field_name in repl_keys:
            if field_name in schema.get("properties", {}):
                stream_metadata_map = metadata.write(
                    stream_metadata_map,
                    ("properties", field_name),
                    "inclusion",
                    "automatic",
                )
            else:
                LOGGER.warning(
                    f"Replication key '{field_name}' for stream '{stream_name}' not found in schema properties."
                )

        field_metadata[stream_name] = metadata.to_list(stream_metadata_map)

    return schemas, field_metadata
