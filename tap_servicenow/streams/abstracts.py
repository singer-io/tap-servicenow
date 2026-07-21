from abc import ABC, abstractmethod
import json
from typing import Any, Dict, NoReturn, Optional, Tuple, List, Iterator
import singer
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    metadata
)

from tap_servicenow.datetime_utils import to_snow_dt
from tap_servicenow.exceptions import (
    ServiceNowError,
    ServiceNowForbiddenError,
    ServiceNowIncompleteSyncError,
    ServiceNowUnauthorizedError,
)


LOGGER = get_logger()


def _raise_permission_error(exc: Exception, stream_name: str, endpoint: str) -> NoReturn:
    """Raise a typed permission error with stream/endpoint context."""
    message = (
        f"Permission error while syncing stream '{stream_name}' on "
        f"endpoint '{endpoint}': {exc}"
    )
    response = getattr(exc, "response", None)
    if isinstance(exc, ServiceNowUnauthorizedError):
        raise ServiceNowUnauthorizedError(message, response) from exc
    raise ServiceNowForbiddenError(message, response) from exc


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    # Page size between 500-2000 per ServiceNow community best practice.
    page_size = 1000
    next_page_key = ""
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    children = []
    parent = ""
    data_key = "result"
    parent_bookmark_key = ""
    http_method = "GET"

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        if catalog:
            self.schema = catalog.schema.to_dict()
            self.metadata = metadata.to_map(catalog.metadata)
        else:
            self.schema = {"type": "object", "properties": {}}
            self.metadata = metadata.new()
        self.child_to_sync = []
        self.params = {}
        self.data_payload = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> List:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    def is_selected(self):
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def get_records(self) -> Iterator:
        """
        Fetch records using offset-based pagination driven by X-Total-Count.

        A lightweight probe request (sysparm_limit=1, no sysparm_no_count) is
        made first to obtain the X-Total-Count response header, which reflects
        the full table size BEFORE row-level ACL filtering.  Pagination then
        continues until ``offset >= total_count`` rather than stopping on the
        first empty page.  This correctly handles tables where ServiceNow ACLs
        hide rows mid-table: an empty page in the middle does NOT mean
        end-of-data (ServiceNow KB0727636), and records at higher offsets may
        still be accessible.

        When X-Total-Count is unavailable (virtual tables), falls back to
        stopping on the first completely empty page.

        Every data request includes the three ServiceNow performance params:
        - sysparm_no_count=true    – skips the expensive COUNT query
        - sysparm_exclude_reference_link=true – trims payload size
        - sysparm_fields           – fetches only schema-selected columns

        Used primarily by FullTableStream.  IncrementalStream.sync() has its
        own inline loop that combines the compound watermark with keyset
        pagination into a single cursor.
        """
        page_size = self.page_size or 1000

        # url_endpoint is set by FullTableStream.sync before it iterates, but
        # get_records is also callable directly.
        endpoint: str = self.url_endpoint or self.get_url_endpoint()
        fields: str = self.selected_fields()

        # ── Step 1: probe for total record count ─────────────────────────────
        try:
            total_count = self.client.get_total_count(
                endpoint, self.params.copy(), self.headers
            )
        except (ServiceNowForbiddenError, ServiceNowUnauthorizedError) as e:
            _raise_permission_error(e, self.tap_stream_id, endpoint)


        # ── Step 2: offset-paginate through the full range ───────────────────
        offset = 0

        # Guard against a server that ignores sysparm_offset and keeps serving
        # the same page. That is what a query_range ACL denial looks like: HTTP
        # 200 with the pagination clause silently dropped. Track the sys_ids on
        # the previous page; a page that repeats it means the cursor is not
        # advancing.
        #
        # This has to run on BOTH paths, not just the no-total_count one:
        #
        #  - Without a count, the only stop condition is an empty page, so a
        #    repeated page loops forever.
        #  - WITH a count the loop does terminate, at ceil(total/page_size)
        #    iterations, but it emits the same page every time and exits 0. Since
        #    targets upsert on sys_id, the destination keeps one page of a table
        #    that may be far larger, and the run reports success. That is the
        #    same silent truncation this module exists to prevent, and it is the
        #    COMMON path - ServiceNow returns X-Total-Count on ordinary tables,
        #    so total_count is normally set and the guard was normally off.
        #
        # No false positives either way: under healthy offset pagination
        # consecutive pages address disjoint row ranges, so their sys_id sets
        # cannot be equal, and an all-hidden page yields an empty signature that
        # `and signature` already excludes.
        seen_page_signature: Optional[frozenset] = None

        while True:
            if total_count is not None and offset >= total_count:
                break

            try:
                paginated_params = self.params.copy()
                paginated_params["sysparm_offset"] = offset
                paginated_params["sysparm_limit"] = page_size
                paginated_params["sysparm_no_count"] = "true"
                paginated_params["sysparm_exclude_reference_link"] = "true"
                if fields:
                    paginated_params["sysparm_fields"] = fields

                try:
                    response = self.client.make_request(
                        self.http_method,
                        endpoint,
                        paginated_params,
                        self.headers,
                        body=json.dumps(self.data_payload),
                        path=self.path,
                    )
                except (ServiceNowForbiddenError, ServiceNowUnauthorizedError) as e:
                    LOGGER.critical(
                        "Permission error on %s: %s. Aborting this stream.",
                        endpoint, e,
                    )
                    _raise_permission_error(e, self.tap_stream_id, endpoint)

                raw_records = response.get(self.data_key, [])

                # Detect a non-advancing cursor BEFORE emitting, so a repeated
                # page is never sent downstream twice.
                signature = frozenset(
                    r.get("sys_id", "") for r in raw_records if r
                )
                if signature and signature == seen_page_signature:
                    raise ServiceNowIncompleteSyncError(
                        f"Stream '{self.tap_stream_id}' stopped before the end of "
                        f"its data: the server returned the same page of "
                        f"{len(raw_records)} row(s) at offset {offset}, so "
                        f"sysparm_offset is not advancing. The table is NOT "
                        f"fully replicated."
                    )
                seen_page_signature = signature

                for record in raw_records:
                    if record:  # skip empty {} records
                        yield record

                # Fallback when total_count is unknown: stop on first empty page.
                if total_count is None and not raw_records:
                    break

                offset += page_size

            except (ServiceNowForbiddenError, ServiceNowUnauthorizedError):
                raise
            except Exception as e:
                LOGGER.error("Unexpected error while fetching records: %s", e)
                raise


    def write_schema(self) -> None:
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def update_params(self, **kwargs) -> None:
        """
        Update params for the stream
        """
        self.params.update(kwargs)

    def update_data_payload(self, **kwargs) -> None:
        """
        Update JSON body for the stream
        """
        self.data_payload.update(kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Get the URL endpoint for the stream
        """
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"

    def selected_fields(self) -> str:
        """Comma-separated `sysparm_fields` value honoring catalog field selection.

        A deselected field is not requested from ServiceNow at all, instead of
        being fetched over the wire and dropped only at output. This mirrors how
        the database taps build their column list (desired_columns /
        should_sync_column) and is the one per-record payload lever the API
        offers. Key and replication-key fields are always retained so keyset
        pagination and bookmarking keep working; `automatic` and unspecified
        fields default to selected (matching should_sync_field default=True).
        """
        props = list(self.schema.get("properties", {}).keys())
        selected = []
        for field in props:
            breadcrumb = ("properties", field)
            inclusion = metadata.get(self.metadata, breadcrumb, "inclusion")
            is_selected = metadata.get(self.metadata, breadcrumb, "selected")
            if inclusion == "unsupported":
                continue
            if inclusion == "automatic" or is_selected is not False:
                selected.append(field)
        # Always keep the keyset cursor and replication key regardless of selection.
        for required in list(self.key_properties or []) + list(self.replication_keys or []):
            if required and required in props and required not in selected:
                selected.append(required)
        return ",".join(selected)


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""


    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(state, stream, key or self.replication_keys[0], self.client.config["start_date"])
        value = max(current_bookmark, value)
        return write_bookmark(
            state, stream, key or self.replication_keys[0], value
        )


    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Incremental sync using a sys_updated_on bookmark combined with keyset
        pagination.  The query always uses >= so the bookmark row may be
        re-read on the next sync.
        """
        replication_key = self.replication_keys[0] if self.replication_keys else "sys_updated_on"

        # --- Retrieve bookmark --------------------------------------------
        bookmark_dt: str = to_snow_dt(
            self.get_bookmark(state, self.tap_stream_id),
            strict=True,
            context=f"the bookmark of stream '{self.tap_stream_id}'",
        )
        current_max_dt: str = bookmark_dt

        page_size: int = self.page_size or 1000
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        if parent_obj:
            self.update_data_payload(**parent_obj)

        # Field selection derived from the stream's schema
        fields: str = self.selected_fields()

        with metrics.record_counter(self.tap_stream_id) as counter:
            empty_record_count = 0
            skipped_uncursorable = 0
            # Keyset cursor: track the last (sys_updated_on, sys_id) seen so we
            # can advance the query on every page without using offset pagination.
            last_page_dt: str = ""
            last_page_sid: str = ""
            has_more: bool = True
            cursor_stalled: bool = False
            try:
                while has_more:
                    if last_page_dt and last_page_sid:
                        query = (
                            f"{replication_key}>={bookmark_dt}"
                            f"^{replication_key}>{last_page_dt}"
                            f"^NQ{replication_key}>={bookmark_dt}"
                            f"^{replication_key}={last_page_dt}"
                            f"^sys_id>{last_page_sid}"
                            f"^ORDERBY{replication_key}^ORDERBYsys_id"
                        )
                    else:
                        query = (
                            f"{replication_key}>={bookmark_dt}"
                            f"^ORDERBY{replication_key}^ORDERBYsys_id"
                        )

                    params: Dict = {
                        "sysparm_query": query,
                        "sysparm_limit": page_size,
                        "sysparm_no_count": "true",
                        "sysparm_exclude_reference_link": "true",
                    }
                    if fields:
                        params["sysparm_fields"] = fields

                    try:
                        response = self.client.make_request(
                            self.http_method,
                            self.url_endpoint,
                            params,
                            self.headers,
                            body=json.dumps(self.data_payload),
                            path=self.path,
                        )
                    except (ServiceNowForbiddenError, ServiceNowUnauthorizedError) as e:
                        LOGGER.critical(
                            "Permission error on %s: %s. Aborting this stream.",
                            self.url_endpoint,
                            e,
                        )
                        _raise_permission_error(e, self.tap_stream_id, self.url_endpoint)

                    raw_records = response.get(self.data_key, [])
                    prev_page_dt, prev_page_sid = last_page_dt, last_page_sid
                    readable_on_page = 0

                    for record in raw_records:
                        if isinstance(record, dict) and not record:
                            empty_record_count += 1
                            continue
                        readable_on_page += 1

                        record = self.modify_object(record, parent_obj)

                        raw_dt = record.get(replication_key)
                        record_sid: str = record.get("sys_id", "")

                        # A record with no replication key cannot position the
                        # cursor. Substituting bookmark_dt (the old behavior)
                        # drove last_page_dt BACKWARDS to the bookmark, so the
                        # next query rewound to the start of the range and
                        # re-served the same page - the stream never advanced
                        # and re-read the same rows on every future run. This
                        # is reachable: field-level ACLs answer with HTTP 200
                        # and the field simply omitted.
                        if not raw_dt or not record_sid:
                            skipped_uncursorable += 1
                            continue

                        # Not strict: one malformed row must not abort the
                        # stream, and to_snow_dt warns before passing it through.
                        record_dt: str = to_snow_dt(
                            raw_dt, context=f"stream '{self.tap_stream_id}'"
                        )

                        # Advance the keyset cursor to the last record on this page
                        last_page_dt = record_dt
                        last_page_sid = record_sid

                        if record_dt >= bookmark_dt:
                            transformed_record = transformer.transform(
                                record, self.schema, self.metadata
                            )
                            if self.is_selected():
                                write_record(self.tap_stream_id, transformed_record)
                                counter.increment()

                            # Only advance bookmark for records that were actually emitted
                            if record_dt > current_max_dt:
                                current_max_dt = record_dt

                            for child in self.child_to_sync:
                                child.sync(
                                    state=state,
                                    transformer=transformer,
                                    parent_obj=record,
                                )

                    # Short pages are expected under ServiceNow row-level ACLs and
                    # do NOT signal end-of-data (KB0727636); only an empty page
                    # does. Stopping on a short page here silently drops records
                    # the bookmark then skips over. The cursor-advance check
                    # prevents an infinite loop on a page that yields no
                    # advanceable (sys_updated_on, sys_id).
                    cursor_advanced = (
                        last_page_dt != prev_page_dt or last_page_sid != prev_page_sid
                    )
                    has_more = bool(raw_records) and cursor_advanced

                    # A non-empty page that does not advance the cursor leaves us
                    # with nowhere to go: the next request would repeat this one.
                    # Only an EMPTY page means end-of-data (KB0727636) - a page
                    # with rows on it does not, whatever those rows contain. Two
                    # ways to get here, both of which strand the sync mid-table:
                    #
                    #  - readable rows that repeat: sys_id is a unique primary
                    #    key, so the same trailing (sys_updated_on, sys_id) twice
                    #    means ServiceNow served the same page twice. That is what
                    #    a query_range ACL denial looks like - HTTP 200 with the
                    #    range clauses silently stripped from the query.
                    #  - rows that are all masked to {}: field-level ACLs can
                    #    leave a row with no readable fields, so the page carries
                    #    no cursor value even though rows exist and more pages
                    #    follow.
                    #
                    # Either way the bookmark must not move: everything past this
                    # page would be skipped forever on a run reporting success.
                    if raw_records and not cursor_advanced:
                        cursor_stalled = True
                        LOGGER.critical(
                            "Stream '%s': keyset cursor did not advance past "
                            "(%s, %s) on a page of %d row(s), %d of them readable. "
                            "ServiceNow may be masking every row on the page, or "
                            "dropping the range clauses from the query "
                            "(query_range ACL). Stopping - this sync is "
                            "INCOMPLETE and the bookmark will not be advanced.",
                            self.tap_stream_id, last_page_dt, last_page_sid,
                            len(raw_records), readable_on_page,
                        )

                if empty_record_count > 0:
                    LOGGER.warning(
                        "Stream '%s' encountered %d empty records "
                        "(possibly due to missing data-level permissions).",
                        self.tap_stream_id, empty_record_count
                    )

                if skipped_uncursorable > 0:
                    LOGGER.warning(
                        "Stream '%s' skipped %d record(s) missing '%s' or "
                        "'sys_id'. Those fields position the keyset cursor, so "
                        "such records cannot be replicated incrementally - "
                        "usually a field-level ACL hiding them.",
                        self.tap_stream_id, skipped_uncursorable, replication_key
                    )

                if cursor_stalled:
                    # Deliberately not writing the bookmark: re-reading this
                    # range next run is cheap, skipping it is permanent. Raising
                    # rather than returning a count, because a short record set
                    # is indistinguishable from a completed sync to the caller.
                    raise ServiceNowIncompleteSyncError(
                        f"Stream '{self.tap_stream_id}' stopped before the end of "
                        f"its data: the keyset cursor stalled at "
                        f"('{last_page_dt}', '{last_page_sid}'). Bookmark left at "
                        f"'{bookmark_dt}'; {counter.value} record(s) were emitted "
                        f"but the table is NOT fully replicated."
                    )

                state = write_bookmark(
                    state, self.tap_stream_id, replication_key, current_max_dt
                )
                singer.write_state(state)
                return counter.value

            except (ServiceNowForbiddenError, ServiceNowUnauthorizedError,
                    ServiceNowIncompleteSyncError):
                raise

            except ServiceNowError as e:
                # A ServiceNow API error that exhausted retries or is non-retryable
                # (excluding permission failures). Log and skip this stream gracefully.
                LOGGER.critical("Skipping stream '%s' due to: %s", self.tap_stream_id, e)
                return 0


class FullTableStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_keys = []

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value


class ParentBaseStream(IncrementalStream):
    """Base Class for Parent Stream."""

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

        min_parent_bookmark = (
            super().get_bookmark(state, stream) if self.is_selected() else None
        )
        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            child_bookmark = super().get_bookmark(
                state, child.tap_stream_id, key=bookmark_key
            )
            min_parent_bookmark = (
                min(min_parent_bookmark, child_bookmark)
                if min_parent_bookmark
                else child_bookmark
            )

        return min_parent_bookmark

    def write_bookmark(
        self, state: Dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            super().write_bookmark(
                state, child.tap_stream_id, key=bookmark_key, value=value
            )

        return state
