from abc import ABC, abstractmethod
import json
from typing import Any, Dict, NoReturn, Tuple, List, Iterator
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
        Fetch records using **keyset pagination** (sys_id-based) instead of
        offset-based pagination.  Offset pagination degrades linearly because
        the database must re-scan and discard all preceding rows; keyset
        pagination stays O(1) per page regardless of position.

        Every request includes the three ServiceNow performance params:
        - sysparm_no_count=true    – skips the expensive COUNT query
        - sysparm_exclude_reference_link=true – trims payload size
        - sysparm_fields           – fetches only schema-selected columns

        Used primarily by FullTableStream.  IncrementalStream.sync() has its
        own inline loop that combines the compound watermark with keyset
        pagination into a single cursor.
        """
        page_size = self.page_size or 1000
        last_sys_id: str = ""
        has_more: bool = True

        # url_endpoint is set by FullTableStream.sync before it iterates, but
        # get_records is also callable directly. Resolve the same fallback
        # make_request uses so an error raised from here names the URL the
        # request actually went to rather than an empty string.
        endpoint: str = self.url_endpoint or self.get_url_endpoint()

        # Build field selection from the schema defined on this stream
        fields: str = self.selected_fields()

        while has_more:
            try:
                paginated_params = self.params.copy()

                # Keyset clause appended to whatever base query was set externally
                base_query = paginated_params.get("sysparm_query", "")
                if last_sys_id:
                    keyset = f"sys_id>{last_sys_id}"
                    paginated_params["sysparm_query"] = (
                        f"{base_query}^{keyset}^ORDERBYsys_id"
                        if base_query
                        else f"{keyset}^ORDERBYsys_id"
                    )
                else:
                    paginated_params["sysparm_query"] = (
                        f"{base_query}^ORDERBYsys_id" if base_query else "ORDERBYsys_id"
                    )

                # Remove offset key if it was added by legacy code
                paginated_params.pop("sysparm_offset", None)

                # Performance params
                paginated_params["sysparm_limit"] = page_size
                paginated_params["sysparm_no_count"] = "true"
                paginated_params["sysparm_exclude_reference_link"] = "true"
                if fields:
                    paginated_params["sysparm_fields"] = fields

                response = self.client.make_request(
                    self.http_method,
                    endpoint,
                    paginated_params,
                    self.headers,
                    body=json.dumps(self.data_payload),
                    path=self.path,
                )
                raw_records = response.get(self.data_key, [])
                prev_sys_id = last_sys_id
                readable_on_page = 0

                for record in raw_records:
                    if record:  # skip empty {} records
                        readable_on_page += 1
                        last_sys_id = record.get("sys_id", last_sys_id)
                        yield record

                # Row-level ACLs are applied after the query, so a short page is
                # expected and does NOT mean end-of-data (ServiceNow KB0727636).
                # Stop only on an empty page; without this, any ACL-filtered short
                # page silently truncates the table. The cursor-advance check
                # guards against an all-empty page looping forever.
                has_more = bool(raw_records) and last_sys_id != prev_sys_id

                # sys_id is a unique primary key, so a page carrying readable
                # records must move the cursor. If it did not, ServiceNow served
                # the same page twice - which is what happens when a query_range
                # ACL denial strips the `sys_id>` clause and answers HTTP 200.
                # There is no cursor left to follow, so stopping is right, but
                # the caller must not read this as a complete table.
                if not has_more and readable_on_page and last_sys_id == prev_sys_id:
                    LOGGER.critical(
                        "Stream '%s': keyset cursor did not advance past sys_id "
                        "'%s' despite %d readable record(s) on the page. "
                        "ServiceNow may be dropping the range clause from the "
                        "query (query_range ACL). This table is INCOMPLETE.",
                        self.tap_stream_id, last_sys_id, readable_on_page,
                    )

            except (ServiceNowForbiddenError, ServiceNowUnauthorizedError) as e:
                LOGGER.critical(
                    "Permission error on %s: %s. Aborting this stream.",
                    endpoint,
                    e,
                )
                _raise_permission_error(e, self.tap_stream_id, endpoint)

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
        bookmark_dt: str = to_snow_dt(self.get_bookmark(state, self.tap_stream_id))
        current_max_dt: str = bookmark_dt

        page_size: int = self.page_size or 1000
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        if parent_obj:
            self.update_data_payload(**parent_obj)

        # Field selection derived from the stream's schema
        fields: str = self.selected_fields()

        with metrics.record_counter(self.tap_stream_id) as counter:
            empty_record_count = 0
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

                        record_dt: str = to_snow_dt(record.get(replication_key) or bookmark_dt)
                        record_sid: str = record.get("sys_id", "")

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

                    # A page carrying readable records must advance the cursor:
                    # sys_id is a unique primary key, so two consecutive pages
                    # ending on the same (sys_updated_on, sys_id) means we were
                    # served the same page twice. That happens when ServiceNow
                    # drops our range clauses instead of honoring them - a
                    # query_range ACL denial is answered with HTTP 200 and the
                    # offending clause silently removed, which strips both the
                    # bookmark filter and the keyset cursor.
                    #
                    # Stopping here is right (there is no cursor left to follow),
                    # but advancing the bookmark is not: everything past this page
                    # would be skipped forever on a run that reported success.
                    # A stall with no readable records is the ordinary
                    # ACL-hidden-rows case and is reported via empty_record_count.
                    if not cursor_advanced and readable_on_page:
                        cursor_stalled = True
                        LOGGER.critical(
                            "Stream '%s': keyset cursor did not advance past "
                            "(%s, %s) despite %d readable record(s) on the page. "
                            "ServiceNow may be dropping the range clauses from "
                            "the query (query_range ACL). Stopping and leaving "
                            "the bookmark unchanged - this sync is incomplete.",
                            self.tap_stream_id, last_page_dt, last_page_sid,
                            readable_on_page,
                        )

                if cursor_stalled:
                    # Deliberately not writing the bookmark: re-reading this
                    # range next run is cheap, skipping it is permanent.
                    LOGGER.warning(
                        "Stream '%s': bookmark left at '%s' because the sync did "
                        "not complete.",
                        self.tap_stream_id, bookmark_dt,
                    )
                else:
                    state = write_bookmark(
                        state, self.tap_stream_id, replication_key, current_max_dt
                    )
                    singer.write_state(state)

                if empty_record_count > 0:
                    LOGGER.warning(
                        "Stream '%s' encountered %d empty records "
                        "(possibly due to missing data-level permissions).",
                        self.tap_stream_id, empty_record_count
                    )
                return counter.value

            except (ServiceNowForbiddenError, ServiceNowUnauthorizedError):
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
