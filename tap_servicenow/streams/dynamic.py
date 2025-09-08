from typing import Dict, List, Tuple
from tap_servicenow.streams.abstracts import FullTableStream
from singer import metadata, get_logger

LOGGER = get_logger()


class DynamicServiceNowTableStream(FullTableStream):
    def __init__(self, client, catalog_entry, table_name):
        self.table_name = table_name
        self.tap_stream_id = table_name
        self.name = table_name
        self.path = f"table/{table_name}"  # used in BaseStream
        super().__init__(client, catalog_entry)

        self.schema = self.get_schema()
        self.metadata = metadata.to_map(catalog_entry.metadata)

    @property
    def replication_method(self) -> str:
        return "FULL_TABLE"

    @property
    def key_properties(self) -> Tuple[str]:
        return ("sys_id",)

    def get_schema(self) -> Dict:
        # Hardcoded minimal schema — you can later expand using ServiceNow schema endpoint
        return {
            "type": "object",
            "properties": {
                "sys_id": {"type": ["string", "null"]},
                "sys_updated_on": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
                # Add more if required or fetch dynamically
            }
        }

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        instance = self.client.config.get("instance")
        return f"https://{instance}.service-now.com/api/now/table/{self.table_name}"


def get_all_tables(client, page_size=100) -> List[str]:
    """
    Paginate through sys_db_object to get all available table names.
    """
    all_tables = []
    offset = 0
    seen = set()

    while True:
        params = {
            "sysparm_offset": offset,
            "sysparm_limit": page_size
        }

        LOGGER.info(f"Fetching ServiceNow tables at offset {offset}")
        response = client.make_request(
            method="GET",
            endpoint=f"https://{client.config['instance']}.service-now.com/api/now/table/sys_db_object",
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

        all_tables.extend(new_names)
        seen.update(new_names)
        offset += len(records)  # Safe next offset

    return all_tables
