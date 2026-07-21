"""concurrent_discovery.py — Speeds up ServiceNow catalog discovery using threads.

Splits the two slowest discovery phases (sys_dictionary fetching and
per-table schema building) across a ThreadPoolExecutor so many ServiceNow
API calls run in parallel instead of sequentially.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import singer
from singer import metadata

from tap_servicenow.datetime_utils import to_snow_dt
from tap_servicenow.exceptions import (
    ServiceNowForbiddenError,
    ServiceNowIncompleteSyncError,
    ServiceNowUnauthorizedError,
)
from tap_servicenow.streams import servicenow_type_to_json_type

LOGGER = singer.get_logger()


def _build_access_probe_params(has_replication_key: bool, start_date: Optional[str]) -> Dict[str, Any]:
    """Build a discovery probe that exercises the same incremental query path as sync.

    Putting sys_updated_on in `sysparm_query` is what makes this probe useful:
    ServiceNow read-checks fields referenced in a query and returns 403
    ("Field(s) present in the query do not have permission to be read"), so a
    table that can be listed but not filtered/ordered by the replication key is
    rejected here rather than failing mid-sync.

    `sysparm_fields` only trims the probe payload. It does NOT detect a
    field-level ACL denial: ServiceNow answers those with HTTP 200 and simply
    omits the field, and with `sysparm_limit=1` an empty result set is
    indistinguishable from a denied field, so there is no safe body-level check.
    """
    params: Dict[str, Any] = {
        "sysparm_limit": 1,
        "sysparm_no_count": "true",
        "sysparm_exclude_reference_link": "true",
    }
    if not has_replication_key:
        return params

    bookmark_dt = to_snow_dt(
        start_date or "", strict=True, context="config start_date"
    )
    if bookmark_dt:
        params["sysparm_query"] = (
            f"sys_updated_on>={bookmark_dt}"
            f"^ORDERBYsys_updated_on^ORDERBYsys_id"
        )
    else:
        params["sysparm_query"] = "ORDERBYsys_updated_on^ORDERBYsys_id"

    params["sysparm_fields"] = "sys_id,sys_updated_on"
    return params


class ConcurrentDiscovery(ABC):
    """Drives a ServiceNow discovery phase by executing API calls in parallel threads."""

    #: When True, an exception from process_item aborts the whole phase instead of
    #: dropping just that item. Set on phases whose failure corrupts the catalog
    #: for every table (the sys_dictionary fetch); left False where skipping one
    #: item is the intended behavior (the per-table schema build, which drops
    #: tables the account cannot read).
    FAIL_FAST: bool = False

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
                    if self.FAIL_FAST:
                        # Cancel what has not started yet before leaving the
                        # `with` block. Without this, ThreadPoolExecutor.__exit__
                        # calls shutdown(wait=True) and the "abort" still works
                        # through every queued chunk first - with hundreds of
                        # chunks at max_workers=10 that is a long wait to report
                        # a failure we already know about.
                        cancelled = sum(1 for f in futures if f.cancel())
                        if cancelled:
                            LOGGER.error(
                                "Aborting discovery phase: cancelled %d queued "
                                "item(s).", cancelled
                            )
                        raise
        return results


class ServiceNowDictionaryFetcher(ConcurrentDiscovery):
    """Fetches field definitions from sys_dictionary for all discovered ServiceNow tables.

    Groups tables into chunks and fires each chunk as a parallel API request
    using the nameIN encoded-query operator.
    """

    # A failed chunk would leave up to chunk_size tables in the catalog with
    # silently missing columns, so abort discovery instead of degrading it.
    FAIL_FAST = True

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
        pages_fetched = 0
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
                # client.make_request already retries transients (RETRY_ON_TRANSIENT,
                # max_tries=5, honors Retry-After), so a failure here is persistent.
                # Returning the rows gathered so far would put these tables in the
                # catalog with silently missing columns, and those columns would then
                # be absent from the destination with no error anywhere. A failed
                # discovery is visible and retryable; a truncated schema is neither.
                LOGGER.error(
                    "sys_dictionary batch query failed for chunk starting %r "
                    "after %d page(s): %s",
                    chunk[:3],
                    pages_fetched,
                    exc,
                )
                raise

            pages_fetched += 1
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

            # sys_dictionary is an ordinary ACL-protected table, so ServiceNow
            # applies row-level ACLs AFTER the query here too: a short page does
            # NOT mean the chunk is exhausted (KB0727636). Stop only on an empty
            # page, matching get_all_tables, BaseStream.get_records and
            # IncrementalStream.sync.
            if not rows:
                break

            # A page that does not advance the cursor strands us: the next
            # request would repeat this one. Returning `partial` would hand back
            # a TRUNCATED field map, and those columns then go missing from the
            # catalog and the destination with no error anywhere - the exact
            # failure the raise above exists to prevent, reached without an
            # exception, so FAIL_FAST cannot catch it either.
            #
            # This raises whatever the page size was. An earlier version only
            # escalated on a FULL page and treated a short stalled page as the
            # end of the chunk, on the theory that sys_dictionary is not
            # row-ACL-filtered the way data rows are. It is. Measured on a dev
            # instance: 151,477 rows by X-Total-Count against 128,210 readable,
            # so 15.4% are ACL-hidden, and 151 of 152 pages came back short of
            # the requested 1,000 with data still behind them (813, 855, 856,
            # 831, ...). That is the same shape as an ordinary table like `task`,
            # not like an unfiltered one like `cmdb_ci`. So page length says
            # nothing about whether the chunk is exhausted, and a short stalled
            # page cannot be read as end-of-data.
            #
            # Normal termination never reaches here: a chunk that is genuinely
            # finished returns an empty page and breaks above. Reaching a stall
            # means the page had rows but none carried a usable sys_id, which is
            # an anomaly worth failing on rather than silently truncating.
            if last_sys_id == prev_sys_id:
                raise ServiceNowIncompleteSyncError(
                    f"sys_dictionary paging stalled at sys_id "
                    f"'{last_sys_id}' after {pages_fetched} page(s) for chunk "
                    f"starting {chunk[:3]!r}: the page returned "
                    f"{len(rows)} row(s) but none advanced the cursor. Page "
                    f"length does not indicate end-of-data here, because "
                    f"sys_dictionary is row-ACL-filtered like any other table "
                    f"(KB0727636), so the field map for these tables would be "
                    f"incomplete. Failing discovery rather than emitting a "
                    f"truncated schema."
                )
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
        self.unauthorized_tables: List[str] = []
        #: (table, error) for tables dropped by something OTHER than a
        #: permission denial. Kept separate so a transient failure is never
        #: reported to the operator as "you lack access".
        self.errored_tables: List[Tuple[str, str]] = []
        self._unauth_lock = threading.Lock()

    def _resolve_fields(self, table_name: str) -> Dict:
        """Merge a table's own fields with everything it inherits via super_class.

        Walks the inheritance chain iteratively and takes no locks while doing
        so. `table_map` and `field_map` are built before the thread pool starts
        and are read-only from here, so the walk needs no synchronization.

        The previous version recursed while holding a per-table lock, which
        deadlocked across threads on a cyclic chain: with A->B->A, a thread
        entering at A holds lock[A] and wants lock[B] while a thread entering
        at B holds lock[B] and wants lock[A]. Classic ABBA, and since
        ThreadPoolExecutor has no timeout, discovery hung forever with no
        output. The old `_visiting` guard could not prevent it - it was a
        per-call-chain frozenset, so it only stopped a thread from re-entering
        its own chain, never two threads from blocking each other.

        Resolving iteratively also makes the result deterministic. Under the
        old scheme a cycle produced different schemas depending on which thread
        cached first.
        """
        # Chain is [table, parent, grandparent, ...]; `seen` breaks any cycle.
        chain: List[str] = []
        seen = set()
        current = table_name
        while current and current not in seen:
            seen.add(current)
            chain.append(current)
            current = self.table_map.get(current, "")

        if current:
            # Loop exited because we came back to a table already in the chain.
            LOGGER.warning(
                "Cyclic super_class chain detected at table '%s' (chain: %s). "
                "Breaking the cycle; inherited fields may be incomplete.",
                current, " -> ".join(chain),
            )

        with self._cache_lock:
            if table_name in self._resolve_cache:
                return self._resolve_cache[table_name]

        # Merge root-first so a child's own definition overrides its parent's.
        merged: Dict = {}
        for name in reversed(chain):
            merged.update(self.field_map.get(name, {}))

        with self._cache_lock:
            # Another thread may have computed the same value concurrently.
            # Both results are identical, so either is fine to keep.
            self._resolve_cache.setdefault(table_name, merged)
            return self._resolve_cache[table_name]

    def _readable_fields(self, table: str, field_names: List[str]) -> Optional[Set[str]]:
        """Return the subset of `field_names` ServiceNow actually returns, or None.

        ServiceNow answers a field-level ACL denial with HTTP 200 and simply
        OMITS the field from the record - it never returns 403 for one. (A 403
        only happens when a denied field is named in `sysparm_query`.) So the
        only reliable signal is which keys come back on a real row: ask for
        every field at once and compare requested against returned.

        A readable-but-empty field still comes back as a key with "", so an
        absent key means denied rather than merely blank.

        Returns None when the table has no rows, because an empty result set
        carries no information about field permissions - callers must treat
        that as "unknown" and keep every field rather than strip them.
        """
        response = self.client.make_request(
            method="GET",
            endpoint=f"{self.client.base_url}/{table}",
            params={
                "sysparm_fields": ",".join(field_names),
                "sysparm_limit": 1,
                "sysparm_no_count": "true",
                "sysparm_exclude_reference_link": "true",
            },
        )
        rows = [r for r in response.get("result", []) if r]
        if not rows:
            return None
        return set(rows[0].keys())

    def _check_field_permissions(
        self, table: str, fields: Dict
    ) -> Tuple[Dict, List[str]]:
        """Drop fields the account cannot read. One request, no bisection.

        The response names every readable field, so there is nothing to search
        for - a single call answers it for the whole table. `sys_id` is always
        retained: it is the primary key and the pagination cursor.

        Fails OPEN. If the table is empty or the probe errors, every field is
        kept. Keeping a field we cannot verify costs a null column; dropping a
        readable one silently removes data from the destination.
        """
        field_names = [f for f in fields if f != "sys_id"]
        if not field_names:
            return dict(fields), []

        try:
            readable = self._readable_fields(table, ["sys_id"] + field_names)
        except Exception as exc:
            LOGGER.warning(
                "Table '%s': field-permission probe failed (%s). Keeping all fields.",
                table, exc,
            )
            return dict(fields), []

        if readable is None:
            LOGGER.debug(
                "Table '%s' has no rows; cannot determine field permissions. "
                "Keeping all fields.", table,
            )
            return dict(fields), []

        unauthorized = [f for f in field_names if f not in readable]
        authorized = {k: v for k, v in fields.items() if k not in unauthorized}
        return authorized, unauthorized

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
            #
            # Incremental streams are probed with the same replication-key query
            # shape sync uses, because ServiceNow read-checks fields named in
            # sysparm_query and 403s there. That 403 says the table cannot be
            # FILTERED by sys_updated_on - it does NOT say the table is
            # unreadable, so fall back to the plain probe before giving up and
            # replicate as FULL_TABLE if the plain read works.
            try:
                self.client.get(
                    table=table,
                    params=_build_access_probe_params(
                        has_replication_key,
                        self.client.config.get("start_date"),
                    ),
                )
            except (ServiceNowForbiddenError, ServiceNowUnauthorizedError):
                if not has_replication_key:
                    # No query clause was involved, so this is a plain read
                    # denial: the table is deliberately excluded, and schema.py
                    # reports the whole set.
                    with self._unauth_lock:
                        self.unauthorized_tables.append(table)
                    return None
                try:
                    self.client.get(
                        table=table,
                        params={"sysparm_limit": 1, "sysparm_no_count": "true"},
                    )
                except (ServiceNowForbiddenError, ServiceNowUnauthorizedError):
                    with self._unauth_lock:
                        self.unauthorized_tables.append(table)
                    return None
                LOGGER.info(
                    "Table '%s' is readable but cannot be filtered by "
                    "sys_updated_on; replicating it as FULL_TABLE.",
                    table,
                )
                has_replication_key = False
                replication_method = "FULL_TABLE"
                valid_replication_keys = []
            except Exception as exc:
                # NOT a permission answer - a 5xx that exhausted its retries, a
                # timeout, a malformed response. Dropping the table here made a
                # transient outage look identical to "the account cannot read
                # this", and because it was never recorded as unauthorized,
                # schema.py's all-tables-blocked check never fired either. The
                # table simply vanished from the catalog, and any stream the
                # customer had selected silently stopped being replicated.
                with self._unauth_lock:
                    self.errored_tables.append((table, str(exc)))
                LOGGER.error(
                    "Error accessing table '%s' (not a permission error): %s. "
                    "Excluding it from this catalog.", table, exc
                )
                return None

            # Field-level permission check: drop fields the account cannot read
            # so the catalog does not advertise columns that will always be
            # absent from the records.
            LOGGER.debug(
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

            # Re-check the replication key: if sys_updated_on itself came back
            # unreadable, an incremental sync cannot bookmark on it.
            if replication_method == "INCREMENTAL" and "sys_updated_on" not in properties:
                LOGGER.warning(
                    "Table '%s': sys_updated_on is not readable; "
                    "switching to FULL_TABLE replication.",
                    table,
                )
                has_replication_key = False
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
            # Same reasoning as the probe's catch-all above: record it so the
            # caller can tell "excluded on purpose" from "broke while building".
            with self._unauth_lock:
                self.errored_tables.append((table, str(exc)))
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
