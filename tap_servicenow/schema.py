import os
import json
import singer
from typing import Dict, Tuple
from singer import metadata
from tap_servicenow.streams import STREAMS
from tap_servicenow.streams import get_all_tables, get_sync_tables, servicenow_type_to_json_type
from tap_servicenow.exceptions import ServiceNowForbiddenError, ServiceNowUnauthorizedError

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
    schemas: Dict = {}
    field_metadata: Dict = {}
    unauthorized_tables = []

    # ------------------------------------------------------------------
    # Step 1: Enumerate all tables (full map for inheritance) then filter
    # ------------------------------------------------------------------
    LOGGER.info("Enumerating tables from sys_db_object (keyset pagination)...")
    table_map: Dict[str, str] = get_all_tables(client)          # {name: super_class}
    sync_tables = get_sync_tables(table_map, config)            # filtered list
    LOGGER.info(
        f"Discovered {len(table_map)} total tables; "
    )

    # ------------------------------------------------------------------
    # Step 2: Collect every table name needed for inheritance resolution
    #         (sync tables PLUS all their ancestors, even excluded ones)
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
    # Step 3: Batch-fetch sys_dictionary for all needed tables
    #         Using nameIN<list> to minimise API round-trips.
    # ------------------------------------------------------------------
    DICT_CHUNK_SIZE = 50       # tables per sys_dictionary request
    DICT_PAGE_SIZE  = 10000    # fields per page (large tables have many fields)

    # field_map[table_name][element_name] = json_schema_type
    field_map: Dict[str, Dict] = {}

    all_needed_list = sorted(all_needed)
    LOGGER.info(
        f"Fetching sys_dictionary for {len(all_needed_list)} unique tables "
        f"in chunks of {DICT_CHUNK_SIZE}..."
    )
    for i in range(0, len(all_needed_list), DICT_CHUNK_SIZE):
        chunk = all_needed_list[i : i + DICT_CHUNK_SIZE]
        names_in = ",".join(chunk)
        params = {
            "sysparm_query": f"nameIN{names_in}",
            "sysparm_fields": "name,element,internal_type",
            "sysparm_limit": DICT_PAGE_SIZE,
            "sysparm_no_count": "true",
            "sysparm_exclude_reference_link": "true",
        }
        try:
            response = client.make_request(
                method="GET",
                endpoint=f"{client.base_url}/sys_dictionary",
                params=params,
            )
        except Exception as exc:
            LOGGER.warning(f"sys_dictionary batch query failed for chunk {i}: {exc}")
            continue

        for field in response.get("result", []):
            tbl   = field.get("name") or ""
            elem  = field.get("element") or ""
            stype = field.get("internal_type") or ""
            if isinstance(stype, dict):
                stype = stype.get("value") or ""
            if not tbl or not elem or not stype:
                continue
            field_map.setdefault(tbl, {})[elem] = servicenow_type_to_json_type(stype)

    # ------------------------------------------------------------------
    # Step 4: Resolve inherited fields by walking the super_class chain
    # ------------------------------------------------------------------
    _resolve_cache: Dict[str, Dict] = {}

    def resolve_fields(table_name: str, _visiting: set = None) -> Dict:
        """Return merged field dict for *table_name* including all ancestors.
        Ancestor fields are overridden by descendant fields (child wins)."""
        if table_name in _resolve_cache:
            return _resolve_cache[table_name]
        visiting = _visiting or set()
        if table_name in visiting:    # cycle guard
            return {}
        visiting = visiting | {table_name}

        own_fields = field_map.get(table_name, {}).copy()
        super_class = table_map.get(table_name, "")
        if super_class:
            parent_fields = resolve_fields(super_class, visiting)
            # Parent provides the base; child fields override
            merged = {**parent_fields, **own_fields}
        else:
            merged = own_fields

        _resolve_cache[table_name] = merged
        return merged

    # ------------------------------------------------------------------
    # Step 5: Build schemas + Singer metadata; verify table access
    # ------------------------------------------------------------------
    for table in sync_tables:
        try:
            properties = resolve_fields(table)

            if not properties:
                LOGGER.warning(f"No fields found for table '{table}'. Skipping.")
                continue

            properties.setdefault("sys_id", {"type": ["string", "null"]})

            has_replication_key = "sys_updated_on" in properties
            if has_replication_key:
                replication_method = "INCREMENTAL"
                valid_replication_keys = ["sys_updated_on"]
            else:
                # Table has no sys_updated_on — treat as FULL_TABLE.
                # Every sync re-reads all rows; no bookmark is written.
                replication_method = "FULL_TABLE"
                valid_replication_keys = []
                LOGGER.debug(
                    f"Table '{table}' has no sys_updated_on field; "
                    f"using FULL_TABLE replication."
                )

            schema = {"type": "object", "properties": properties}

            # Lightweight access check (1 record, no count)
            try:
                client.get(
                    table=table,
                    params={"sysparm_limit": 1, "sysparm_no_count": "true"},
                )
            except (ServiceNowForbiddenError, ServiceNowUnauthorizedError):
                unauthorized_tables.append(table)
                continue
            except Exception as exc:
                LOGGER.warning(f"Error accessing table '{table}': {exc}")
                continue

            schemas[table] = schema

            mdata = metadata.get_standard_metadata(
                schema=schema,
                key_properties=["sys_id"],
                valid_replication_keys=valid_replication_keys,
                replication_method=replication_method,
            )
            mdata = metadata.to_map(mdata)
            if has_replication_key:
                mdata = metadata.write(
                    mdata, ("properties", "sys_updated_on"), "inclusion", "automatic"
                )
            field_metadata[table] = metadata.to_list(mdata)

        except Exception as exc:
            LOGGER.error(f"Failed to build schema for table '{table}': {exc}")
            continue

    # ------------------------------------------------------------------
    # Step 6: Deferred unauthorised-table summary (PR #1 pattern kept)
    # ------------------------------------------------------------------
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
