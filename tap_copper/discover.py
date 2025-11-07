import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from typing import Dict

from tap_copper.schema import get_schemas

LOGGER = singer.get_logger()


def discover(config: Dict = None) -> Catalog:
    schemas, field_metadata = get_schemas(config=config)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: %s", stream_name)
            LOGGER.error("type schema_dict: %s", type(schema_dict))
            raise

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
