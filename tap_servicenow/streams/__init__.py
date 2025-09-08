from typing import Dict, Type
from tap_servicenow.streams.dynamic import DynamicServiceNowTableStream

# Global registry of all stream classes
STREAMS: Dict[str, Type[DynamicServiceNowTableStream]] = {}

def register_dynamic_stream(table_name: str):
    """
    Dynamically register a stream class based on table name.
    """
    if table_name not in STREAMS:
        STREAMS[table_name] = DynamicServiceNowTableStream
