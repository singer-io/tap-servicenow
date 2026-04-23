"""
concurrent_discovery.py
~~~~~~~~~~~~~~~~~~~~~~~
Generic concurrent discovery framework and ServiceNow-specific implementations.

Generic base class
------------------
:class:`ConcurrentDiscovery` is a reusable, tap-agnostic ABC.  To use it in
another tap:

1. Subclass it and implement :meth:`process_item`.
2. Build the list of "items" to process (table names, endpoint slugs, …).
3. Call :meth:`run` — results are returned as a plain list, in completion order.

ServiceNow classes
------------------
* :class:`ServiceNowDictionaryFetcher` — parallelises ``sys_dictionary`` batch
  chunk requests (Step 3 of discovery).
* :class:`ServiceNowTableSchemaBuilder` — parallelises per-table schema
  construction and lightweight access probes (Steps 4+5 of discovery).

Thread-safety notes
-------------------
``requests.Session`` is not officially thread-safe, but the only mutable
session state set here is ``_session.auth``, which every thread writes to the
same value (username/password never change mid-run).  The underlying
``urllib3`` connection pool IS thread-safe for concurrent reads, so the
pattern works reliably in practice for read-only discovery.  If you need
strict thread-safety, inject one session-per-thread via a thread-local or use
a fresh ``Client`` per worker.
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


# ---------------------------------------------------------------------------
# Generic reusable base
# ---------------------------------------------------------------------------

class ConcurrentDiscovery(ABC):
    """
    Generic base class for running discovery tasks concurrently across a
    collection of items.

    Subclasses implement :meth:`process_item` to define the work done for each
    individual item.  :meth:`run` submits all items to a
    ``ThreadPoolExecutor``, collects non-``None`` results, and returns them
    in completion order.

    The base class shares **no** mutable state between worker threads.
    Subclasses that maintain shared state (caches, error accumulators, …) are
    responsible for protecting it with appropriate locks — see
    :class:`ServiceNowTableSchemaBuilder` for a reference implementation.

    Parameters
    ----------
    max_workers:
        Maximum number of concurrent worker threads.  Tune this to stay within
        the upstream API's rate limits.  Defaults to 10.

    Example (minimal)::

        class MyDiscovery(ConcurrentDiscovery):
            def process_item(self, item):
                return fetch_schema(item)   # return None to skip an item

        results = MyDiscovery(max_workers=20).run(my_items)
    """

    def __init__(self, max_workers: int = 10) -> None:
        self.max_workers = max_workers

    @abstractmethod
    def process_item(self, item: Any) -> Optional[Any]:
        """
        Process a single item and return a result, or ``None`` to skip it.

        This method is called concurrently from worker threads.  Exceptions
        raised here are caught by :meth:`run`, logged as errors, and the item
        is silently skipped — they do **not** propagate to the caller.
        """

    def run(self, items: Iterable) -> List:
        """
        Submit all *items* to the thread pool and return collected results.

        Parameters
        ----------
        items:
            Any iterable of items to pass to :meth:`process_item`.

        Returns
        -------
        List
            Non-``None`` results from :meth:`process_item`, in completion
            order (non-deterministic across runs).
        """
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


# ---------------------------------------------------------------------------
# ServiceNow: parallel sys_dictionary fetcher
# ---------------------------------------------------------------------------

class ServiceNowDictionaryFetcher(ConcurrentDiscovery):
    """
    Fetches ``sys_dictionary`` field definitions for a large set of ServiceNow
    table names using concurrent API calls.

    Discovery previously issued one HTTP request per table; this class batches
    tables with the ``nameIN<list>`` encoded-query operator (one request per
    *chunk_size* tables) and fires all chunks concurrently, dramatically
    reducing wall-clock time.

    Parameters
    ----------
    client:
        An initialised tap client exposing ``make_request`` and ``base_url``.
    dict_page_size:
        ``sysparm_limit`` per API call — must be large enough to cover all
        fields in the widest table in a chunk.  Defaults to 10 000.
    max_workers:
        Concurrency level.  Defaults to 10.
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

    # ------------------------------------------------------------------
    # ConcurrentDiscovery interface
    # ------------------------------------------------------------------

    def process_item(self, chunk: List[str]) -> Optional[Dict[str, Dict]]:
        """
        Fetch ``sys_dictionary`` rows for the table names in *chunk* (one API
        call).  Returns a partial ``{table: {field: json_type}}`` dict, or
        ``None`` if the request fails.
        """
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

    # ------------------------------------------------------------------
    # High-level entry point
    # ------------------------------------------------------------------

    def fetch(
        self,
        table_names: List[str],
        chunk_size: int = 50,
    ) -> Dict[str, Dict]:
        """
        Fetch field definitions for all *table_names* concurrently.

        Splits *table_names* into chunks of *chunk_size*, fires all chunks
        through the thread pool, then merges the partial results.

        Parameters
        ----------
        table_names:
            Sorted list of table names to look up in ``sys_dictionary``.
        chunk_size:
            Number of tables per individual API request.  Larger values mean
            fewer round-trips but larger response payloads.

        Returns
        -------
        Dict[str, Dict]
            ``field_map[table_name][element_name]`` → JSON Schema type dict.
        """
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


# ---------------------------------------------------------------------------
# ServiceNow: parallel table schema builder
# ---------------------------------------------------------------------------

class ServiceNowTableSchemaBuilder(ConcurrentDiscovery):
    """
    Builds Singer catalog schemas and metadata for a list of ServiceNow tables
    concurrently, including a lightweight per-table access probe.

    This class owns the field-inheritance resolution logic (walking the
    ``super_class`` chain) and uses a thread-safe cache so that ancestor
    lookups are computed at most once across all concurrent workers.

    Parameters
    ----------
    client:
        An initialised tap client exposing ``get`` (access probe) and the
        ``config`` dict.
    field_map:
        Pre-fetched ``{table_name: {field_name: json_type}}`` mapping, e.g.
        as returned by :meth:`ServiceNowDictionaryFetcher.fetch`.
    table_map:
        Full ``{table_name: super_class_name}`` map for inheritance
        resolution, as returned by ``get_all_tables``.
    max_workers:
        Concurrency level.  Defaults to 10.

    Attributes
    ----------
    unauthorized_tables : List[str]
        Tables skipped because the configured credentials returned 401/403.
        Populated after :meth:`build` returns.
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
        self.unauthorized_tables: List[str] = []
        self._unauth_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Inheritance resolution (thread-safe)
    # ------------------------------------------------------------------

    def _resolve_fields(
        self,
        table_name: str,
        _visiting: Optional[frozenset] = None,
    ) -> Dict:
        """
        Return the merged field dict for *table_name* including all ancestor
        fields.  Results are memoised in ``_resolve_cache`` (thread-safe).
        Child fields override parent fields (child wins).

        Uses ``frozenset`` for the visitation set so it can be safely shared
        across recursive, potentially-concurrent calls without defensive
        copies.
        """
        with self._cache_lock:
            if table_name in self._resolve_cache:
                return self._resolve_cache[table_name]

        visiting = _visiting or frozenset()
        if table_name in visiting:   # cycle guard
            return {}
        visiting = visiting | {table_name}

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

    # ------------------------------------------------------------------
    # ConcurrentDiscovery interface
    # ------------------------------------------------------------------

    def process_item(self, table: str) -> Optional[Dict]:
        """
        Build a Singer schema + metadata entry for *table* and verify API
        access.

        Returns a dict with keys ``"table"``, ``"schema"``, ``"metadata"``,
        or ``None`` if the table should be skipped (no fields, access denied,
        or any unhandled error).
        """
        try:
            properties = self._resolve_fields(table)

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

    # ------------------------------------------------------------------
    # High-level entry point
    # ------------------------------------------------------------------

    def build(self, tables: List[str]) -> Tuple[Dict, Dict]:
        """
        Concurrently process all *tables* and return their schemas and Singer
        metadata.

        Parameters
        ----------
        tables:
            List of table names to build schemas for.

        Returns
        -------
        Tuple[Dict, Dict]
            ``(schemas, field_metadata)`` dicts keyed by table name.
            Tables that were skipped (no fields, access denied, errors) are
            absent from both dicts.  Unauthorised tables are also recorded in
            :attr:`unauthorized_tables`.
        """
        LOGGER.info(
            "ServiceNowTableSchemaBuilder: processing %d tables "
            "(max_workers=%d)",
            len(tables),
            self.max_workers,
        )
        results = self.run(tables)

        schemas: Dict = {}
        field_metadata: Dict = {}
        for item in results:
            schemas[item["table"]] = item["schema"]
            field_metadata[item["table"]] = item["metadata"]
        return schemas, field_metadata
