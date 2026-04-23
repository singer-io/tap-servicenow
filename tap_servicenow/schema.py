import os
import json
import singer
from typing import Dict, Tuple
from singer import metadata
from tap_servicenow.streams import STREAMS
from tap_servicenow.streams import get_all_tables, get_sync_tables
from tap_servicenow.concurrent_discovery import (
    ServiceNowDictionaryFetcher,
    ServiceNowTableSchemaBuilder,
)

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

    Key improvements over the previous implementation:

    1. **Table filtering** – default exclusion list (audit/log tables) applied
       via :func:`get_sync_tables`; configurable via ``include_tables`` /
       ``exclude_tables`` config keys.
    2. **Batch sys_dictionary queries** – fields are fetched for up to
       DICT_CHUNK_SIZE tables per API call using the ``nameIN`` encoded-query
       operator, replacing ~12,600 individual calls from the prior version.
    3. **Table-inheritance resolution** – the super_class chain returned by
       :func:`get_all_tables` is walked so that fields inherited from ancestor
       tables (e.g. ``incident`` → ``task``) are merged into child schemas,
       preventing silent data loss.

    Returns:
        Tuple[Dict, Dict]: (schemas, field_metadata)
    """
    LOGGER.info("Fetching dynamic schema from ServiceNow.")
    config = getattr(client, "config", {})
    max_workers = int(config.get("discovery_max_workers", 10))

    # ------------------------------------------------------------------
    # Enumerate all tables (full map for inheritance) then filter
    # ------------------------------------------------------------------
    LOGGER.info("Enumerating tables from sys_db_object (keyset pagination)...")
    table_map: Dict[str, str] = get_all_tables(client)          # {name: super_class}
    sync_tables = get_sync_tables(table_map, config)            # filtered list
    LOGGER.info(
        f"Discovered {len(table_map)} total tables; "
    )

    # ------------------------------------------------------------------
    # Collect every table name needed for inheritance resolution
    # (sync tables PLUS all their ancestors, even excluded ones)
    # ------------------------------------------------------------------
    all_needed: set = set(sync_tables)
    for name in sync_tables:
        current = table_map.get(name, "")
        visited: set = {name}
        while current and current not in visited:
            all_needed.add(current)
            visited.add(current)
            current = table_map.get(current, "")

    # ------------------------------------------------------------------
    # Batch-fetch sys_dictionary for all needed tables (concurrent)
    # ------------------------------------------------------------------
    DICT_CHUNK_SIZE = 50   # tables per sys_dictionary request

    all_needed_list = sorted(all_needed)
    field_map: Dict[str, Dict] = ServiceNowDictionaryFetcher(
        client, max_workers=max_workers
    ).fetch(all_needed_list, chunk_size=DICT_CHUNK_SIZE)

    # ------------------------------------------------------------------
    # Build schemas + Singer metadata concurrently
    #             (inheritance resolution + access probe per table)
    # ------------------------------------------------------------------
    builder = ServiceNowTableSchemaBuilder(
        client, field_map, table_map, max_workers=max_workers
    )
    schemas, field_metadata = builder.build(sync_tables)

    # ------------------------------------------------------------------
    # Deferred unauthorised-table summary
    # ------------------------------------------------------------------
    unauthorized_tables = builder.unauthorized_tables
    if unauthorized_tables:
        total   = len(sync_tables)
        blocked = len(unauthorized_tables)
        tables_str = ", ".join(unauthorized_tables)
        if blocked < total:
            LOGGER.warning(
                f"Credentials lack access to {blocked} table(s): {tables_str}. "
                f"These tables were skipped due to insufficient permissions."
            )
        else:
            raise Exception(
                "HTTP-error-code: 403. The account does not have 'read' access "
                "to any of the ServiceNow tables. Data discovery cannot proceed."
            )

    return schemas, field_metadata
