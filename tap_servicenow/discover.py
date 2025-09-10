import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry
from tap_servicenow.streams.dynamic import get_all_tables, DynamicServiceNowTableStream

LOGGER = singer.get_logger()

def discover(client) -> Catalog:
    """
    Dynamically discover all tables from ServiceNow and build the catalog.
    """

    LOGGER.info("Starting dynamic discovery of ServiceNow tables")

    table_names = get_all_tables(client)
    streams = []

    for table_name in table_names:
        stream = DynamicServiceNowTableStream(client, None, table_name)

        # Start metadata for stream-level
        md = metadata.new()

        md = metadata.write(md, (), 'table-key-properties', list(stream.key_properties))
        md = metadata.write(md, (), 'replication-method', stream.replication_method)
        md = metadata.write(md, (), 'forced-replication-method', stream.replication_method)
        md = metadata.write(md, (), 'inclusion', 'automatic')
        md = metadata.write(md, (), 'selected', 'true')

        for field_name in stream.schema_dict.get("properties", {}):
            md = metadata.write(md, ("properties", field_name), "inclusion", "automatic")

        catalog_entry = CatalogEntry(
            stream=stream.name,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.key_properties,
            schema=stream.schema,
            metadata=metadata.to_list(md),
            replication_method=stream.replication_method
        )
        streams.append(catalog_entry)

    LOGGER.info(f"Discovered {len(streams)} tables")
    return Catalog(streams=streams)
