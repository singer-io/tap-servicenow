from typing import Dict, List, Tuple, Optional
from tap_servicenow.streams.abstracts import IncrementalStream
from singer import metadata, get_logger
from singer.schema import Schema

LOGGER = get_logger()


class DynamicServiceNowTableStream(IncrementalStream):
    def __init__(self, client, catalog_entry: Optional[dict], table_name: str):
        self.table_name = table_name
        self.name = table_name
        self.path = f"table/{table_name}"
        super().__init__(client, catalog_entry)
        self._raw_schema = self.get_schema()
        self._dynamic_schema = Schema(self._raw_schema)

        if catalog_entry:
            self.metadata = metadata.to_map(catalog_entry.metadata)
        else:
            self.metadata = {}

    @property
    def schema(self) -> Schema:
        return self._dynamic_schema
    @schema.setter
    def schema(self, value: Dict) -> None:
        self._dynamic_schema = Schema(value)
        
    @property
    def schema_dict(self) -> Dict:
        full_schema = self._dynamic_schema.to_dict()
        if "type" in full_schema and isinstance(full_schema["type"], dict):
            inner = full_schema["type"]
            if "properties" in inner:
                return {"properties": inner["properties"]}
        return full_schema
    
    @property
    def tap_stream_id(self) -> str:
        return self.table_name

    @property
    def replication_method(self) -> str:
        return "INCREMENTAL"

    @property
    def key_properties(self) -> list:
        return ["sys_id"]

    @property
    def replication_keys(self) -> list:
        return ["sys_updated_on"]

    def get_schema(self) -> Dict:
        """
        Dynamically fetch schema for the ServiceNow table using sys_dictionary.
        """
        LOGGER.info(f"Fetching schema for table: {self.table_name}")

        params = {
            "sysparm_query": f"name={self.table_name}",
            "sysparm_fields": "element,internal_type",
            "sysparm_limit": 1000
        }

        try:
            response = self.client.make_request(
                method="GET",
                endpoint=f"{self.client.base_url}/table/sys_dictionary",
                params=params
            )
            fields = response.get("result", [])

            if not fields:
                LOGGER.warning(f"No schema fields returned for table: {self.table_name}")
                # Fallback minimal schema
                return {
                    "type": "object",
                    "properties": {
                        "sys_id": {"type": ["string", "null"]}
                    }
                }

            schema = {
                "type": "object",
                "properties": {}
            }

            for field in fields:
                name = field.get("element")
                internal_type = field.get("internal_type")

                # Fix: unwrap dict if necessary
                if isinstance(internal_type, dict):
                    internal_type = internal_type.get("value")

                if not name or not internal_type:
                    continue

                json_type = self.servicenow_type_to_json_type(internal_type)
                schema["properties"][name] = {
                    "type": json_type
                }

            return schema

        except Exception as e:
            LOGGER.error(f"Failed to fetch schema for table {self.table_name}: {str(e)}")
            raise

    @staticmethod
    def servicenow_type_to_json_type(snow_type: str) -> List[str]:
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

        return mapping.get(snow_type.lower(), ["string", "null"])


def get_all_tables(client, page_size=100, max_tables: int = None) -> List[str]:
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
            endpoint=f"{client.base_url}/table/sys_db_object",
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

    return all_tables
