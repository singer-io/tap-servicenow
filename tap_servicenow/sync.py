import singer
from typing import Dict
from singer import metadata
from tap_servicenow.streams import STREAMS, abstracts
from tap_servicenow.client import Client
from tap_servicenow.exceptions import (
    ServiceNowForbiddenError,
    ServiceNowUnauthorizedError,
)
from tap_servicenow.streams.abstracts import IncrementalStream, FullTableStream


LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update currently_syncing in state and write it
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_schema(stream, client, streams_to_sync, catalog) -> None:
    """
    Write schema for stream and its children
    """
    if stream.is_selected():
        stream.write_schema()

    for child in stream.children:
        child_obj = STREAMS[child](client, catalog.get_stream(child))
        write_schema(child_obj, client, streams_to_sync, catalog)
        if child in streams_to_sync:
            stream.child_to_sync.append(child_obj)


def build_dynamic_stream(client, catalog_entry: singer.CatalogEntry) -> object:
    """Create a dynamic stream instance based on stream_catalog."""
    catalog_metadata = metadata.to_map(catalog_entry.metadata)

    stream_name = catalog_entry.stream
    key_properties = catalog_entry.key_properties
    replication_method = catalog_metadata.get((), {}).get('forced-replication-method')
    replication_keys = catalog_metadata.get((), {}).get('valid-replication-keys')

    class_props = {
        "__module__": abstracts.__name__,
        "tap_stream_id": property(lambda self: stream_name),
        "key_properties": property(lambda self: key_properties),
        "replication_method": property(lambda self: replication_method),
        "replication_keys": property(lambda self: replication_keys),
        "path": stream_name,
        "data_key": "result",
        "is_dynamic": True
    }

    base_class = FullTableStream if replication_method == "FULL_TABLE" else IncrementalStream

    DynamicStreamClass = type(
        f"Dynamic{stream_name.title()}Stream",
        (base_class,),
        class_props
    )
    # This is safe because DynamicStreamClass is created at runtime with all required abstract methods
    # implemented via the selected base class (IncrementalStream) and class_props.
    return DynamicStreamClass(client, catalog_entry) # pylint: disable=abstract-class-instantiated


def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:
    """
    Sync selected streams from catalog
    """

    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.stream)
    LOGGER.info("selected_streams: {}".format(streams_to_sync))

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: {}".format(last_stream))

    # Streams whose sync aborted on a permission error. Collected rather than
    # raised immediately so one unreadable table cannot cost us every stream
    # after it: ServiceNow evaluates row-level ACLs after the query runs
    # (KB0727636), so a table can pass the discovery probe and still 403
    # part-way through a sync. The run still fails at the end (below) so the
    # job is not reported as successful.
    permission_failures = []

    with singer.Transformer() as transformer:
        for stream_name in streams_to_sync:
            stream = build_dynamic_stream(client, catalog.get_stream(stream_name))
            parent_name = getattr(stream, "parent", None)
            if parent_name:
                if parent_name not in streams_to_sync:
                    streams_to_sync.append(parent_name)
                continue

            write_schema(stream, client, streams_to_sync, catalog)
            LOGGER.info("START Syncing: {}".format(stream_name))
            update_currently_syncing(state, stream_name)
            try:
                total_records = stream.sync(state=state, transformer=transformer)
            except (ServiceNowForbiddenError, ServiceNowUnauthorizedError) as e:
                # The stream raised before writing its bookmark, so nothing was
                # advanced past rows we never fetched. Record it and move on.
                # The exception already carries the stream and endpoint, so this
                # only adds what happens next.
                LOGGER.critical("%s Continuing with the remaining streams.", e)
                permission_failures.append(stream_name)
                update_currently_syncing(state, None)
                continue

            update_currently_syncing(state, None)
            LOGGER.info(
                "FINISHED Syncing: {}, total_records: {}".format(
                    stream_name, total_records
                )
            )

    if permission_failures:
        raise ServiceNowForbiddenError(
            "The account lacks 'read' access to {} of {} selected stream(s): {}. "
            "Bookmarks for these streams were left unchanged; all other streams "
            "synced successfully.".format(
                len(permission_failures),
                len(streams_to_sync),
                ", ".join(permission_failures),
            )
        )
