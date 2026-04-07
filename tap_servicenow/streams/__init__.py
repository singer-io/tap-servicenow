from typing import Dict, List, Optional, Union
from singer import get_logger

LOGGER = get_logger()

STREAMS = {}

# Tables excluded from sync by default due to high row counts, high write
# velocity, or because they are queried via a dedicated mechanism (e.g.
# sys_audit_delete for delete detection).  Operators may override these via
# the "include_tables" / "exclude_tables" config keys.
DEFAULT_EXCLUDED_TABLES: frozenset = frozenset({
    "sys_audit",            # millions of rows; Fivetran explicitly blocks this
    "sys_audit_delete",     # queried separately for delete detection; not a data table
    "syslog",               # transaction logs; millions of rows, high write velocity
    "syslog_transaction",   # per-transaction performance logs
    "sys_email_log",        # email delivery logs; high volume on active instances
    "sys_history_line",     # field-level change history; row count proportional to all field changes
    "sys_history_set",      # change-set groupings for sys_history_line
    "ha_log",               # high-availability cluster logs
    "sys_cache_flush",      # cache management events
    "sys_cluster_state",    # cluster node state
})


def get_all_tables(client, page_size: int = 500) -> Dict[str, str]:
    """
    Enumerate **all** tables from sys_db_object using keyset pagination
    (sys_id-based, avoids offset degradation on large result sets).

    Returns a dict of ``{table_name: super_class_name}`` for every table
    present in the instance.  No filtering is applied here so that the
    full map can be used for table-inheritance resolution in schema
    discovery.  Call :func:`get_sync_tables` to apply the exclusion list.

    Dot-walking ``super_class.name`` in sysparm_fields returns the
    referenced table's name as a flat string, which is what we need for
    walking the inheritance chain.
    """
    table_map: Dict[str, str] = {}
    last_sys_id: str = ""

    while True:
        if last_sys_id:
            query = f"sys_id>{last_sys_id}^ORDERBYsys_id"
        else:
            query = "ORDERBYsys_id"

        params = {
            "sysparm_query": query,
            "sysparm_fields": "name,sys_id,super_class.name",
            "sysparm_limit": page_size,
            "sysparm_no_count": "true",
            "sysparm_exclude_reference_link": "true",
        }

        response = client.make_request(
            method="GET",
            endpoint=f"{client.base_url}/sys_db_object",
            params=params,
        )

        records = response.get("result", [])
        if not records:
            break

        for r in records:
            name = r.get("name") or ""
            # dot-walked field arrives as a plain string or nested dict
            super_raw = r.get("super_class.name") or r.get("super_class") or ""
            if isinstance(super_raw, dict):
                super_raw = super_raw.get("display_value") or super_raw.get("value") or ""
            sys_id = r.get("sys_id") or ""

            if name:
                table_map[name] = super_raw
            if sys_id:
                last_sys_id = sys_id

        if len(records) < page_size:
            break

    return table_map


def get_sync_tables(
    table_map: Dict[str, str],
    config: Optional[Dict] = None,
) -> List[str]:
    """
    Apply the default exclusion list plus any operator-supplied
    ``include_tables`` / ``exclude_tables`` config overrides to
    ``table_map`` and return the ordered list of table names to sync.

    ``include_tables`` (if non-empty) acts as an allowlist — only those
    tables are synced.  ``exclude_tables`` is additive to the default
    exclusion list.
    """
    config = config or {}

    excluded: set = set(DEFAULT_EXCLUDED_TABLES)
    excluded.update(config.get("exclude_tables", []))

    include_only: set = set(config.get("include_tables", []))

    result: List[str] = []
    for name in table_map:
        if name in excluded:
            continue
        if include_only and name not in include_only:
            continue
        result.append(name)

    return result


def servicenow_type_to_json_type(snow_type: str) -> Dict[str, Union[str, List[str]]]:
        """
        Map ServiceNow field types to JSON Schema types.
        """
        mapping = {
            "string": {"type": ["string", "null"]},
            "glide_date_time": {"type": ["string", "null"], "format": "date-time"},
            "glide_date": {"type": ["string", "null"], "format": "date-time"},
            "int": {"type": ["integer", "null"]},
            "integer": {"type": ["integer", "null"]},
            "float": {"type": ["number", "null"]},
            "boolean": {"type": ["boolean", "null"]},
            "reference": {"type": ["string", "null"]},
            "currency": {"type": ["number", "null"]},
            "text": {"type": ["string", "null"]},
            "html": {"type": ["string", "null"]},
            "url": {"type": ["string", "null"]},
            "email": {"type": ["string", "null"]},
            # Add as per need
        }

        return mapping.get(snow_type.lower(), {"type": ["string", "null"]})