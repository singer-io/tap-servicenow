"""concurrent_discovery.py — Speeds up ServiceNow catalog discovery using threads.

Splits the two slowest discovery phases (sys_dictionary fetching and
per-table schema building) across a ThreadPoolExecutor so many ServiceNow
API calls run in parallel instead of sequentially.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple

import singer
from singer import metadata

from tap_servicenow.exceptions import ServiceNowForbiddenError, ServiceNowUnauthorizedError
from tap_servicenow.streams import servicenow_type_to_json_type

LOGGER = singer.get_logger()


class ConcurrentDiscovery(ABC):
    """Drives a ServiceNow discovery phase by executing API calls in parallel threads."""

    def __init__(self, max_workers: int = 10) -> None:
        self.max_workers = max_workers

    @abstractmethod
    def process_item(self, item: Any) -> Optional[Any]:
        """Perform the discovery work for a single item (table or chunk). Return None to skip."""

    def run(self, items: Iterable) -> List:
        """Fire all items through the thread pool and collect non-None results."""
        results: List = []
        items_list = list(items)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_item, item): item
                for item in items_list
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as exc:
                    LOGGER.error(
                        "ConcurrentDiscovery: error processing item %r: %s", item, exc
                    )
        return results


class ServiceNowDictionaryFetcher(ConcurrentDiscovery):
    """Fetches field definitions from sys_dictionary for all discovered ServiceNow tables.

    Groups tables into chunks and fires each chunk as a parallel API request
    using the nameIN encoded-query operator.
    """

    def __init__(
        self,
        client,
        dict_page_size: int = 10_000,
        max_workers: int = 10,
    ) -> None:
        super().__init__(max_workers)
        self.client = client
        self.dict_page_size = dict_page_size

    def process_item(self, chunk: List[str]) -> Optional[Dict[str, Dict]]:
        """Query sys_dictionary for one batch of ServiceNow tables. Returns {table: {field: type}}."""
        names_in = ",".join(chunk)
        params = {
            "sysparm_query": f"nameIN{names_in}",
            "sysparm_fields": "name,element,internal_type",
            "sysparm_limit": self.dict_page_size,
            "sysparm_no_count": "true",
            "sysparm_exclude_reference_link": "true",
        }
        try:
            response = self.client.make_request(
                method="GET",
                endpoint=f"{self.client.base_url}/sys_dictionary",
                params=params,
            )
        except Exception as exc:
            LOGGER.warning(
                "sys_dictionary batch query failed for chunk starting %r: %s",
                chunk[:3],
                exc,
            )
            return None

        partial: Dict[str, Dict] = {}
        for field in response.get("result", []):
            tbl   = field.get("name") or ""
            elem  = field.get("element") or ""
            stype = field.get("internal_type") or ""
            if isinstance(stype, dict):
                stype = stype.get("value") or ""
            if not tbl or not elem or not stype:
                continue
            partial.setdefault(tbl, {})[elem] = servicenow_type_to_json_type(stype)
        return partial

    def fetch(
        self,
        table_names: List[str],
        chunk_size: int = 50,
    ) -> Dict[str, Dict]:
        """Fetch field types for all ServiceNow tables and return {table: {field: json_type}}."""
        chunks = [
            table_names[i : i + chunk_size]
            for i in range(0, len(table_names), chunk_size)
        ]
        LOGGER.info(
            "ServiceNowDictionaryFetcher: fetching %d tables in %d chunks "
            "(max_workers=%d, chunk_size=%d)",
            len(table_names),
            len(chunks),
            self.max_workers,
            chunk_size,
        )
        partial_maps = self.run(chunks)

        field_map: Dict[str, Dict] = {}
        for partial in partial_maps:
            for tbl, fields in partial.items():
                field_map.setdefault(tbl, {}).update(fields)
        return field_map


class ServiceNowTableSchemaBuilder(ConcurrentDiscovery):
    """Builds the Singer catalog schema and metadata for each ServiceNow table in parallel.

    Resolves inherited fields by walking the super_class chain, then runs a
    lightweight API access probe per table. Unauthorized tables are collected
    in self.unauthorized_tables after build() completes.
    """

    def __init__(
        self,
        client,
        field_map: Dict[str, Dict],
        table_map: Dict[str, str],
        max_workers: int = 10,
    ) -> None:
        super().__init__(max_workers)
        self.client = client
        self.field_map = field_map
        self.table_map = table_map

        # Shared state — protected by locks
        self._resolve_cache: Dict[str, Dict] = {}
        self._cache_lock = threading.Lock()
        self._key_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)
        self.unauthorized_tables: List[str] = []
        self._unauth_lock = threading.Lock()

    def _resolve_fields(
        self,
        table_name: str,
        _visiting: Optional[frozenset] = None,
    ) -> Dict:
        """Walk the super_class chain and merge all inherited fields into the table's schema."""
        # Cycle guard — must run before acquiring the per-key lock; otherwise a
        # cyclic inheritance chain (A→B→A) would cause the same thread to try to
        # re-acquire a non-reentrant Lock it already holds → deadlock.
        visiting = _visiting or frozenset()
        if table_name in visiting:
            return {}
        visiting = visiting | {table_name}

        # Fast path: check shared cache first
        with self._cache_lock:
            if table_name in self._resolve_cache:
                return self._resolve_cache[table_name]

        # Acquire the per-key lock — only threads computing THIS table block here
        with self._key_locks[table_name]:
            # Double-check: another thread may have computed it while we waited
            with self._cache_lock:
                if table_name in self._resolve_cache:
                    return self._resolve_cache[table_name]

            # Now we are the sole thread computing this table

            own_fields = self.field_map.get(table_name, {}).copy()
            super_class = self.table_map.get(table_name, "")
            if super_class:
                parent_fields = self._resolve_fields(super_class, visiting)
                merged = {**parent_fields, **own_fields}   # child overrides parent
            else:
                merged = own_fields

            with self._cache_lock:
                self._resolve_cache[table_name] = merged
            return merged

    def process_item(self, table: str) -> Optional[Dict]:
        """Build the Singer schema and metadata for one ServiceNow table and verify API access."""
        try:
            # Copy so we don't mutate the shared inheritance cache
            properties = dict(self._resolve_fields(table))

            if not properties:
                LOGGER.warning("No fields found for table '%s'. Skipping.", table)
                return None

            properties.setdefault("sys_id", {"type": ["string", "null"]})

            has_replication_key = "sys_updated_on" in properties
            if has_replication_key:
                replication_method = "INCREMENTAL"
                valid_replication_keys = ["sys_updated_on"]
            else:
                replication_method = "FULL_TABLE"
                valid_replication_keys = []
                LOGGER.debug(
                    "Table '%s' has no sys_updated_on field; "
                    "using FULL_TABLE replication.",
                    table,
                )

            schema = {"type": "object", "properties": properties}

            # Lightweight access probe (1 record, no count)
            try:
                self.client.get(
                    table=table,
                    params={"sysparm_limit": 1, "sysparm_no_count": "true"},
                )
            except (ServiceNowForbiddenError, ServiceNowUnauthorizedError):
                with self._unauth_lock:
                    self.unauthorized_tables.append(table)
                return None
            except Exception as exc:
                LOGGER.warning("Error accessing table '%s': %s", table, exc)
                return None

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

            return {
                "table": table,
                "schema": schema,
                "metadata": metadata.to_list(mdata),
            }

        except Exception as exc:
            LOGGER.error("Failed to build schema for table '%s': %s", table, exc)
            return None

    def build(self, tables: List[str]) -> Tuple[Dict, Dict]:
        """Build Singer schemas and metadata for all ServiceNow sync tables. Returns (schemas, field_metadata)."""
        LOGGER.info(
            "ServiceNowTableSchemaBuilder: processing %d tables "
            "(max_workers=%d)",
            len(tables),
            self.max_workers,
        )
        result_map = {item["table"]: item for item in self.run(tables)}

        # Re-order by the original `tables` list so catalog stream order is
        # deterministic across runs (as_completed yields in random order).
        schemas: Dict = {}
        field_metadata: Dict = {}
        for table in tables:
            if table in result_map:
                schemas[table] = result_map[table]["schema"]
                field_metadata[table] = result_map[table]["metadata"]
        return schemas, field_metadata
