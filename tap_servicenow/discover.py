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

        # Write stream-level metadata
        md = metadata.write(md, (), 'table-key-properties', list(stream.key_properties))
        md = metadata.write(md, (), 'selected', True)
        md = metadata.write(md, (), 'replication-method', stream.replication_method)
        md = metadata.write(md, (), 'forced-replication-method', stream.replication_method)
        md = metadata.write(md, (), 'valid-replication-keys', [])
        md = metadata.write(md, (), 'inclusion', 'available')  # Include stream-level

        # Property-level metadata
        LOGGER.info(f"Schema type: {type(stream.schema)}")
        properties = stream.schema.properties or {}
        LOGGER.info(f"Schema properties for {table_name}: {list(properties.keys())}")

        for field in properties:
            inclusion = 'automatic' if field in stream.key_properties else 'available'
            md = metadata.write(md, ('properties', field), 'inclusion', inclusion)

        for field in properties:
            inclusion = 'automatic' if field in stream.key_properties else 'available'
            md = metadata.write(md, ('properties', field), 'inclusion', inclusion)

        LOGGER.info("%s", metadata.to_list(md))
        # Build catalog entry
        catalog_entry = CatalogEntry(
            stream=table_name,
            tap_stream_id=table_name,
            key_properties=stream.key_properties,
            schema=stream.schema,
            metadata=metadata.to_list(md),  # Convert metadata map to list for Singer
            replication_method=stream.replication_method
        )
        streams.append(catalog_entry)

    LOGGER.info(f"Discovered {len(streams)} tables")
    return Catalog(streams=streams)
