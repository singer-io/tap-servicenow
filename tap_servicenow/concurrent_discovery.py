"""concurrent_discovery.py — Speeds up ServiceNow catalog discovery using threads.

Splits the two slowest discovery phases (sys_dictionary fetching and
per-table schema building) across a ThreadPoolExecutor so many ServiceNow
API calls run in parallel instead of sequentially.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
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
        """Query sys_dictionary for one batch of ServiceNow tables. Returns {table: {field: type}}.

        Keyset-paginates by sys_id: a single fixed-limit request silently drops
        fields for any chunk whose dictionary rows exceed the limit, which would
        leave those tables with incomplete (or empty) schemas.
        """
        names_in = ",".join(chunk)
        base_query = f"nameIN{names_in}"
        partial: Dict[str, Dict] = {}
        last_sys_id = ""
        while True:
            query = (
                f"{base_query}^sys_id>{last_sys_id}^ORDERBYsys_id"
                if last_sys_id else f"{base_query}^ORDERBYsys_id"
            )
            params = {
                "sysparm_query": query,
                "sysparm_fields": "name,element,internal_type,sys_id",
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
                return partial or None

            rows = response.get("result", [])
            prev_sys_id = last_sys_id
            for field in rows:
                sid = field.get("sys_id") or ""
                if sid:
                    last_sys_id = sid
                tbl   = field.get("name") or ""
                elem  = field.get("element") or ""
                stype = field.get("internal_type") or ""
                if isinstance(stype, dict):
                    stype = stype.get("value") or ""
                if not tbl or not elem or not stype:
                    continue
                partial.setdefault(tbl, {})[elem] = servicenow_type_to_json_type(stype)

            # Fetch another page only if we filled this one (may be more rows);
            # a partial page means the chunk is exhausted. The cursor-stall check
            # prevents an infinite loop if a page carries no advanceable sys_id.
            if len(rows) < self.dict_page_size or last_sys_id == prev_sys_id:
                break
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
    per-table read-access probe. Tables the account cannot read are collected in
    self.unauthorized_tables and dropped from the catalog so the QTC selection UI
    only offers readable streams. This is deliberately different from the database
    taps: ServiceNow's metadata (sys_db_object) lists tables whose DATA the account
    cannot read - unlike a database's information_schema - so listing everything
    would flood the picker with unusable streams. The probe uses the client's
    shared retry policy so a transient 429 does not drop a readable table.
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
        self._key_locks: Dict[str, threading.Lock] = {}
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

        # Check cache and atomically create the per-key lock under _cache_lock
        with self._cache_lock:
            if table_name in self._resolve_cache:
                return self._resolve_cache[table_name]
            if table_name not in self._key_locks:
                self._key_locks[table_name] = threading.Lock()
            key_lock = self._key_locks[table_name]

        # Acquire the per-key lock — only threads computing THIS table block here
        with key_lock:
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

    def _check_field_batch(self, table: str, field_names: List[str]) -> bool:
        """Check if a batch of fields is accessible by querying them together.
        
        Uses a query pattern similar to sync to catch query-dependent field permissions.
        ServiceNow's field-level ACLs can be query-dependent - a field might be
        readable with a simple query but not with filter conditions.
        
        Returns:
            bool: True if all fields in batch are accessible, False if any are not
        """
        try:
            # Match sync query pattern: include sys_updated_on filter + ORDER BY
            # Some ACLs are only triggered when filtering on dates/bookmarks
            query_params = {
                "sysparm_fields": ",".join(field_names),
                "sysparm_limit": 1,
                "sysparm_no_count": "true",
                "sysparm_exclude_reference_link": "true",
            }
            
            # If table has sys_updated_on, use the same filter pattern as incremental sync
            # This catches ACLs that are only enforced when filtering by date
            if "sys_updated_on" in field_names or any(f.startswith("sys_") for f in field_names):
                # Use a date filter similar to sync (far past date to match any records)
                query_params["sysparm_query"] = "sys_updated_on>=1970-01-01 00:00:00^ORDERBYsys_updated_on^ORDERBYsys_id"
            else:
                # Fallback to simple ordering
                query_params["sysparm_query"] = "ORDERBYsys_id"
            
            self.client.get(table=table, params=query_params)
            return True
        except ServiceNowForbiddenError:
            return False

    def _find_unauthorized_fields(
        self, table: str, field_names: List[str]
    ) -> List[str]:
        """Use divide-and-conquer to find which fields in a batch are unauthorized.
        
        Returns:
            List[str]: List of unauthorized field names
        """
        if not field_names:
            return []
        
        # Base case: single field
        if len(field_names) == 1:
            if self._check_field_batch(table, field_names):
                return []
            else:
                return field_names
        
        # Divide: split into two halves
        mid = len(field_names) // 2
        left_half = field_names[:mid]
        right_half = field_names[mid:]
        
        unauthorized = []
        
        # Conquer: recursively check each half
        if not self._check_field_batch(table, left_half):
            unauthorized.extend(self._find_unauthorized_fields(table, left_half))
        
        if not self._check_field_batch(table, right_half):
            unauthorized.extend(self._find_unauthorized_fields(table, right_half))
        
        return unauthorized

    def _check_field_permissions(self, table: str, fields: Dict) -> Tuple[Dict, List[str]]:
        """Check field-level read permissions using optimized batch testing.
        
        Strategy:
        1. Test all fields at once (best case: 1 API call)
        2. If that fails, use divide-and-conquer to find unauthorized fields
        3. Binary search minimizes API calls: log2(N) instead of N calls
        
        Returns:
            Tuple[Dict, List[str]]: (authorized_fields, unauthorized_field_names)
        """
        authorized_fields = {}
        unauthorized_fields = []

        # Always include sys_id as it's required for pagination and primary key
        if "sys_id" in fields:
            authorized_fields["sys_id"] = fields["sys_id"]

        # Get list of fields to check (excluding sys_id)
        fields_to_check = [f for f in fields.keys() if f != "sys_id"]
        
        if not fields_to_check:
            return authorized_fields, unauthorized_fields

        try:
            # OPTIMIZATION: Try all fields at once first (best case: 1 API call)
            all_accessible = self._check_field_batch(table, fields_to_check)
            
            if all_accessible:
                # All fields are accessible - include them all
                for field_name in fields_to_check:
                    authorized_fields[field_name] = fields[field_name]
            else:
                # Some fields are unauthorized - use divide-and-conquer to find them
                LOGGER.debug(
                    "Table '%s': Some fields are unauthorized, narrowing down...",
                    table
                )
                unauthorized_fields = self._find_unauthorized_fields(table, fields_to_check)
                
                # Include only authorized fields
                for field_name in fields_to_check:
                    if field_name not in unauthorized_fields:
                        authorized_fields[field_name] = fields[field_name]
                
                # Log each unauthorized field at debug level
                for field_name in unauthorized_fields:
                    LOGGER.debug(
                        "Field '%s' in table '%s' is not accessible due to "
                        "insufficient permissions.",
                        field_name,
                        table,
                    )

        except Exception as exc:
            # For unexpected exceptions, include all fields but log a warning
            LOGGER.warning(
                "Error checking field permissions for table '%s': %s. "
                "Including all fields in schema.",
                table,
                exc,
            )
            for field_name in fields_to_check:
                authorized_fields[field_name] = fields[field_name]

        return authorized_fields, unauthorized_fields

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

            # Per-table read-access probe. ServiceNow lists tables in sys_db_object
            # whose DATA the account cannot read, so unreadable tables are dropped
            # here to keep the QTC selection UI to readable streams. client.get's
            # shared retry policy means a transient 429 retries rather than
            # wrongly dropping a readable table.
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

            # Field-level permission check: test each field individually
            # and remove fields that don't have read permission
            LOGGER.info(
                "Checking field-level permissions for table '%s' (%d fields)...",
                table,
                len(properties),
            )
            authorized_properties, unauthorized_field_names = self._check_field_permissions(
                table, properties
            )

            if unauthorized_field_names:
                LOGGER.warning(
                    "Table '%s': Excluded %d field(s) due to insufficient permissions: %s",
                    table,
                    len(unauthorized_field_names),
                    ", ".join(sorted(unauthorized_field_names)),
                )

            # Use only authorized fields in the schema
            properties = authorized_properties

            if not properties:
                LOGGER.warning(
                    "Table '%s': No accessible fields after permission check. Skipping table.",
                    table,
                )
                with self._unauth_lock:
                    self.unauthorized_tables.append(table)
                return None

            # Re-check replication key after field filtering
            has_replication_key = "sys_updated_on" in properties
            if not has_replication_key and replication_method == "INCREMENTAL":
                LOGGER.warning(
                    "Table '%s': sys_updated_on field not accessible; "
                    "switching to FULL_TABLE replication.",
                    table,
                )
                replication_method = "FULL_TABLE"
                valid_replication_keys = []
            
            schema = {
                "type": "object",
                "properties": properties,
                "additionalProperties": False
            }

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
