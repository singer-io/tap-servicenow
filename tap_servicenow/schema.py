import os
import json
import singer
from typing import Dict, Tuple
from singer import metadata
from tap_servicenow.streams import STREAMS
from tap_servicenow.streams import get_all_tables, servicenow_type_to_json_type

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict:
    """
    Load the schema files from the schema folder and return the schema references.
    """
    shared_schema_path = get_abs_path("schemas/shared")

    shared_file_names = []
    if os.path.exists(shared_schema_path):
        shared_file_names = [
            f
            for f in os.listdir(shared_schema_path)
            if os.path.isfile(os.path.join(shared_schema_path, f))
        ]

    refs = {}
    for shared_schema_file in shared_file_names:
        with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
            refs["shared/" + shared_schema_file] = json.load(data_file)

    return refs


def get_schemas() -> Tuple[Dict, Dict]:
    """
    Load the schema references, prepare metadata for each streams and return schema and metadata for the catalog.
    """
    schemas = {}
    field_metadata = {}

    refs = load_schema_references()
    for stream_name, stream_obj in STREAMS.items():
        schema_path = get_abs_path("schemas/{}.json".format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)

        schemas[stream_name] = schema
        schema = singer.resolve_schema_references(schema, refs)

        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=getattr(stream_obj, "key_properties"),
            valid_replication_keys=(getattr(stream_obj, "replication_keys") or []),
            replication_method=getattr(stream_obj, "replication_method"),
        )
        mdata = metadata.to_map(mdata)

        automatic_keys = getattr(stream_obj, "replication_keys") or []
        for field_name in schema.get("properties", {}).keys():
            if field_name in automatic_keys:
                mdata = metadata.write(
                    mdata, ("properties", field_name), "inclusion", "automatic"
                )

        parent_tap_stream_id = getattr(stream_obj, "parent", None)
        if parent_tap_stream_id:
            mdata = metadata.write(mdata, (), 'parent-tap-stream-id', parent_tap_stream_id)

        mdata = metadata.to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata


def get_dynamic_schema(client) -> Tuple[Dict, Dict]:
    """
    Fetch dynamic schemas and metadata for all ServiceNow tables.
    Returns:
        Tuple[Dict, Dict]: (schemas, field_metadata)
    """
    LOGGER.info("Fetching dynamic schema from ServiceNow.")
    schemas = {}
    field_metadata = {}

    # Step 1: Get all table names from sys_db_object
    table_names = get_all_tables(client)

    for table in table_names:
        try:
            LOGGER.info(f"Processing table: {table}")

            # Step 2: Fetch schema fields from sys_dictionary
            params = {
                "sysparm_query": f"name={table}",
                "sysparm_fields": "element,internal_type",
                "sysparm_limit": 1000
            }

            response = client.make_request(
                method="GET",
                endpoint=f"{client.base_url}/sys_dictionary",
                params=params
            )

            fields = response.get("result", [])
            if not fields:
                LOGGER.warning(f"No fields found for table: {table}. Skipping.")
                continue

            properties = {}
            for field in fields:
                name = field.get("element")
                snow_type = field.get("internal_type")

                if isinstance(snow_type, dict):
                    snow_type = snow_type.get("value")

                if not name or not snow_type:
                    continue

                json_type = servicenow_type_to_json_type(snow_type)
                properties[name] = json_type

            # Ensure sys_id is included
            if "sys_id" not in properties:
                properties["sys_id"] = {"type": ["string", "null"]}
                
            if "sys_updated_on" not in properties:
                properties["sys_updated_on"] = {"type": ["string", "null"], "format": "date-time"}


            # Create schema dict
            schema = {
                "type": "object",
                "properties": properties
            }

            schemas[table] = schema

            try:
                status_code = client.get(
                    table=table,
                    params={"sysparm_limit": 1}
                )

                if status_code in (401, 403):
                    LOGGER.warning(f"Cannot access table '{table}'. Please check your credentials and permissions.")

            except Exception as e:
                LOGGER.warning(f"Error accessing data from table {table}: {str(e)}")
                continue

            # Step 4: Create singer metadata
            mdata = metadata.get_standard_metadata(
                schema=schema,
                key_properties=["sys_id"],
                valid_replication_keys=["sys_updated_on"],
                replication_method="INCREMENTAL"
            )
            mdata = metadata.to_map(mdata)

            # Mark sys_updated_on as automatic
            if "sys_updated_on" in properties:
                mdata = metadata.write(
                    mdata, ("properties", "sys_updated_on"), "inclusion", "automatic"
                )

            field_metadata[table] = metadata.to_list(mdata)

        except Exception as e:
            LOGGER.error(f"Failed to fetch schema for table {table}: {str(e)}")
            continue

    return schemas, field_metadata
