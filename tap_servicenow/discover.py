from typing import Dict
import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_servicenow.schema import get_dynamic_schema
from tap_servicenow.client import Client

LOGGER = singer.get_logger()

def discover(client) -> Catalog:
    """
    Dynamically discover all tables from ServiceNow and build the catalog.
    """

    LOGGER.info("Starting dynamic discovery of ServiceNow tables")

    dynamic_schemas, dynamic_field_metadata = get_dynamic_schema(client)
    catalog = Catalog([])

    for stream_name, schema_dict in dynamic_schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = dynamic_field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: {}".format(stream_name))
            LOGGER.error("type schema_dict: {}".format(type(schema_dict)))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                replication_key='sys_updated_on',
                metadata=mdata,
            )
        )

    return catalog
