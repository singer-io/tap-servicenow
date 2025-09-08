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
        catalog_entry = CatalogEntry(
            stream=table_name,
            tap_stream_id=table_name,
            key_properties=stream.key_properties,
            schema=stream.schema,
            metadata=metadata.to_map([]),  # empty metadata or customize if needed
            replication_method=stream.replication_method
        )
        streams.append(catalog_entry)

    catalog = Catalog(streams=streams)

    LOGGER.info(f"Discovered {len(streams)} tables")
    return catalog
