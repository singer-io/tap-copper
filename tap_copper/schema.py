import json
import os
from typing import Dict, Tuple, Any

import singer
from singer import metadata
import requests

from tap_copper.streams import STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """Get the absolute path for the schema files."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict[str, Any]:
    """
    Load the schema files from the schemas/shared folder and return the $ref references.
    """
    shared_schema_path = get_abs_path("schemas/shared")
    refs: Dict[str, Any] = {}

    if os.path.exists(shared_schema_path):
        for fname in os.listdir(shared_schema_path):
            fpath = os.path.join(shared_schema_path, fname)
            if os.path.isfile(fpath):
                with open(fpath, encoding="utf-8") as data_file:
                    refs["shared/" + fname] = json.load(data_file)

    return refs


def _build_auth_headers(config: Dict) -> Dict[str, str]:
    """
    Replicate Copper auth headers (minimal, for discovery probe).
    We use requests directly to avoid Singer metric noise during discovery.
    """
    return {
        "X-PW-Application": "developer_api",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-PW-AccessToken": config.get("api_key", ""),
        "X-PW-UserEmail": config.get("user_email", ""),
    }


def check_stream_authorization(config: Dict, stream_name: str, stream_obj, mdata: Dict) -> Dict:
    """
    Check if stream is authorized by making a lightweight REST API probe.
    """
    if not config:
        return mdata

    if getattr(stream_obj, "parent", None):
        return mdata

    if not hasattr(stream_obj, "path"):
        return mdata

    base_url = (config.get("base_url") or "https://api.copper.com/developer_api/v1").rstrip("/")
    path = getattr(stream_obj, "path")
    method = getattr(stream_obj, "http_method", "GET").upper()
    url = f"{base_url}/{path}".rstrip("/")

    headers = _build_auth_headers(config)
    timeout = float(config.get("request_timeout", 300)) or 300

    try:
        if method == "GET":
            resp = requests.request(method, url, headers=headers, params={"page_size": 1}, timeout=timeout)
        elif method == "POST":
            resp = requests.request(method, url, headers=headers, json={}, timeout=timeout)
        else:
            return mdata

        if resp.status_code in (401, 403,404):
            LOGGER.warning(f"Cannot access data for '{stream_name}'. Please check your credentials and permissions.")
    except Exception as e:
        LOGGER.error(f"Error testing authorization for stream {stream_name}: {e}")

    return mdata


def get_schemas(config: Dict = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Load the schema references, prepare metadata for each stream, and return schema and metadata for the catalog
    """
    schemas: Dict[str, Any] = {}
    field_metadata: Dict[str, Any] = {}

    refs = load_schema_references()

    for stream_name, stream_obj in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, encoding="utf-8") as file:
            raw_schema = json.load(file)

        schemas[stream_name] = raw_schema

        try:
            resolved_schema = singer.resolve_schema_references(raw_schema, refs)
        except Exception:
            resolved_schema = raw_schema

        mdata_list = metadata.get_standard_metadata(
            schema=resolved_schema,
            key_properties=getattr(stream_obj, "key_properties"),
            valid_replication_keys=(getattr(stream_obj, "replication_keys") or []),
            replication_method=getattr(stream_obj, "replication_method"),
        )
        mdata_map = metadata.to_map(mdata_list)

        automatic_keys = getattr(stream_obj, "replication_keys") or []
        for field_name in resolved_schema.get("properties", {}).keys():
            if field_name in automatic_keys:
                mdata_map = metadata.write(
                    mdata_map, ("properties", field_name), "inclusion", "automatic"
                )

        mdata_map = check_stream_authorization(config, stream_name, stream_obj, mdata_map)
        parent_tap_stream_id = getattr(stream_obj, "parent", None)
        if parent_tap_stream_id:
            mdata_map = metadata.write(mdata_map, (), "parent-tap-stream-id", parent_tap_stream_id)
        field_metadata[stream_name] = metadata.to_list(mdata_map)

    return schemas, field_metadata
