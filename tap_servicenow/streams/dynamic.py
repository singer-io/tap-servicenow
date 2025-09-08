from typing import Dict, List, Tuple, Optional
from tap_servicenow.streams.abstracts import FullTableStream
from singer import metadata, get_logger
from singer.schema import Schema

LOGGER = get_logger()


class DynamicServiceNowTableStream(FullTableStream):
    def __init__(self, client, catalog_entry: Optional[dict], table_name: str):
        self.table_name = table_name
        self.name = table_name
        self.path = f"table/{table_name}"
        super().__init__(client, catalog_entry)

        self._dynamic_schema = Schema(self.get_schema())

        if catalog_entry:
            self.metadata = metadata.to_map(catalog_entry.metadata)
        else:
            self.metadata = {}

    @property
    def schema(self) -> Schema:
        if isinstance(self._dynamic_schema, Schema):
            return self._dynamic_schema
        return Schema(self._dynamic_schema or {"type": "object", "properties": {}})
    
    @schema.setter
    def schema(self, value: Dict) -> None:
        self._dynamic_schema = Schema(value)
    
    @property
    def tap_stream_id(self) -> str:
        return self.table_name

    @property
    def replication_method(self) -> str:
        return "FULL_TABLE"

    @property
    def key_properties(self) -> Tuple[str]:
        return ("sys_id",)

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
            "string": ["string", "null"],
            "glide_date_time": ["string", "null"],
            "glide_date": ["string", "null"],
            "int": ["integer", "null"],
            "integer": ["integer", "null"],
            "float": ["number", "null"],
            "boolean": ["boolean", "null"],
            "reference": ["string", "null"],
            "currency": ["number", "null"],
            "text": ["string", "null"],
            "html": ["string", "null"],
            "url": ["string", "null"],
            "email": ["string", "null"],
            # Add more mappings as needed
        }

        return mapping.get(snow_type.lower(), ["string", "null"])


def get_all_tables(client, page_size=100, max_tables=1) -> List[str]:
    """
    Paginate through sys_db_object to get up to `max_tables` available table names.
    """
    all_tables = []
    offset = 0
    seen = set()

    while len(all_tables) < max_tables:
        params = {
            "sysparm_offset": offset,
            "sysparm_limit": page_size
        }

        LOGGER.info(f"Fetching ServiceNow tables at offset {offset}")
        response = client.make_request(
            method="GET",
            endpoint=f"{client.base_url}/table/sys_db_object",
            params=params
        )

        records = response.get("result", [])
        if not records:
            LOGGER.info("Received empty batch. Ending pagination.")
            break

        new_names = [r["name"] for r in records if "name" in r and r["name"] not in seen]
        if not new_names:
            LOGGER.info("No new table names discovered. Ending.")
            break

        for name in new_names:
            if len(all_tables) >= max_tables:
                break
            all_tables.append(name)
            seen.add(name)

        offset += len(records)

    LOGGER.info(f"Returning {len(all_tables)} tables for testing.")
    return all_tables
