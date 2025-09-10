from typing import Dict, List, Union
from singer import get_logger

LOGGER = get_logger()

STREAMS= {}


def get_all_tables(client, page_size=100, max_tables: int = 1) -> List[str]:
    """
    Paginate through sys_db_object to get up to `max_tables` table names.
    If `max_tables` is None, it fetches all tables (production mode).
    """
    all_tables = []
    offset = 0
    seen = set()

    while True:
        params = {
            "sysparm_offset": offset,
            "sysparm_limit": page_size
        }

        response = client.make_request(
            method="GET",
            endpoint=f"{client.base_url}/sys_db_object",
            params=params
        )

        records = response.get("result", [])
        if not records:
            break

        new_names = [r["name"] for r in records if "name" in r and r["name"] not in seen]
        if not new_names:
            break

        for name in new_names:
            if max_tables is not None and len(all_tables) >= max_tables:
                return all_tables
            all_tables.append(name)
            seen.add(name)

        offset += len(records)
    LOGGER.info('sdhajfkhd %s', all_tables)
    return all_tables


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