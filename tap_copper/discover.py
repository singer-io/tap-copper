import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_copper.schema import get_schemas

LOGGER = singer.get_logger()


def discover() -> Catalog:
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            metadata_list = field_metadata[stream_name]  # Singer metadata as a LIST
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: %s", stream_name)
            LOGGER.error("type schema_dict: %s", type(schema_dict))
            raise err

        metadata_map = metadata.to_map(metadata_list)

        # Keep stream present, but mark unsupported and log the reason
        if stream_name == "pipeline_stages":
            LOGGER.warning(
                "Marking stream '%s' as unsupported during discovery (unauthorized at source).",
                stream_name,
            )
            metadata_map = metadata.write(metadata_map, (), "inclusion", "unsupported")
            metadata_map = metadata.write(metadata_map, (), "selected-by-default", False)

        # Convert back to list for CatalogEntry
        metadata_list = metadata.to_list(metadata_map)

        # Table key properties come from root metadata
        key_properties = metadata.to_map(metadata_list).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=metadata_list,
            )
        )

    return catalog
