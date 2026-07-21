import unittest
from unittest.mock import patch, MagicMock
from tap_servicenow.exceptions import (
    ServiceNowForbiddenError,
    ServiceNowNotFoundError,
    ServiceNowUnauthorizedError,
)
from tap_servicenow.sync import write_schema, sync, update_currently_syncing
from tap_servicenow.streams import STREAMS

class TestSync(unittest.TestCase):

    @patch.dict(STREAMS, {
        "invoice_payments": MagicMock(),
        "invoice_line_items": MagicMock(),
        "invoices": MagicMock(),
        "expenses": MagicMock(),
    })
    def test_write_schema_only_parent_selected(self):
        mock_stream = MagicMock()
        mock_stream.is_selected.return_value = True
        mock_stream.children = ["invoice_payments", "invoice_line_items"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(mock_stream, client, [], catalog)

        mock_stream.write_schema.assert_called_once()
        self.assertEqual(len(mock_stream.child_to_sync), 0)


    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_servicenow.streams.abstracts.IncrementalStream.sync")
    def test_sync_stream1_called(self, mock_sync, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema):
        mock_catalog = MagicMock()
        invoice_stream = MagicMock()
        invoice_stream.stream = "invoices"
        expense_stream = MagicMock()
        expense_stream.stream = "expenses"
        mock_catalog.get_selected_streams.return_value = [
            invoice_stream,
            expense_stream
        ]
        state = {}

        client = MagicMock()
        config = {}

        sync(client, config, mock_catalog, state)

        self.assertEqual(mock_sync.call_count, 2)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_servicenow.streams.abstracts.IncrementalStream.sync")
    def test_sync_child_selected(self, mock_sync, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema):
        mock_catalog = MagicMock()
        invoice_messages_stream = MagicMock()
        invoice_messages_stream.stream = "invoice_messages"
        invoice_payments_stream = MagicMock()
        invoice_payments_stream.stream = "invoice_payments"
        mock_catalog.get_selected_streams.return_value = [
            invoice_messages_stream,
            invoice_payments_stream
        ]
        state = {}

        client = MagicMock()
        config = {}

        sync(client, config, mock_catalog, state)

        self.assertEqual(mock_sync.call_count, 2)

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_remove_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = "some_stream"
        state = {"currently_syncing": "some_stream"}

        update_currently_syncing(state, None)

        mock_get_currently_syncing.assert_called_once_with(state)
        mock_set_currently_syncing.assert_not_called()
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state) 

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_set_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = None
        state = {}

        update_currently_syncing(state, "new_stream")

        mock_get_currently_syncing.assert_not_called()
        mock_set_currently_syncing.assert_called_once_with(state, "new_stream")
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state)


class TestSyncPermissionErrorIsolation(unittest.TestCase):
    """A permission error on one stream must not cost us the streams after it.

    ServiceNow evaluates row-level ACLs after the query runs (KB0727636), so a
    table can pass the discovery probe and still 403 part-way through a sync.
    Aborting the whole run on the first such stream would drop every remaining
    stream's data.
    """

    def _catalog(self, *names):
        catalog = MagicMock()
        streams = []
        for name in names:
            entry = MagicMock()
            entry.stream = name
            streams.append(entry)
        catalog.get_selected_streams.return_value = streams
        return catalog

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_servicenow.streams.abstracts.IncrementalStream.sync")
    def test_later_streams_still_sync_after_permission_error(
        self, mock_sync, *_
    ):
        # First stream 403s, the two after it succeed.
        mock_sync.side_effect = [
            ServiceNowForbiddenError("403 Forbidden"),
            5,
            7,
        ]

        with self.assertRaises(ServiceNowForbiddenError):
            sync(MagicMock(), {}, self._catalog("alpha", "beta", "gamma"), {})

        # All three were attempted, not just the one that failed.
        self.assertEqual(mock_sync.call_count, 3)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_servicenow.streams.abstracts.IncrementalStream.sync")
    def test_run_still_fails_and_names_every_blocked_stream(
        self, mock_sync, *_
    ):
        mock_sync.side_effect = [
            ServiceNowForbiddenError("403 Forbidden"),
            5,
            ServiceNowUnauthorizedError("401 Unauthorized"),
        ]

        with self.assertRaises(ServiceNowForbiddenError) as ctx:
            sync(MagicMock(), {}, self._catalog("alpha", "beta", "gamma"), {})

        message = str(ctx.exception)
        self.assertIn("alpha", message)
        self.assertIn("gamma", message)
        self.assertNotIn("beta", message)   # beta synced fine
        self.assertIn("2 of 3", message)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_servicenow.streams.abstracts.IncrementalStream.sync")
    def test_no_raise_when_every_stream_succeeds(self, mock_sync, *_):
        mock_sync.side_effect = [1, 2]
        sync(MagicMock(), {}, self._catalog("alpha", "beta"), {})
        self.assertEqual(mock_sync.call_count, 2)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_servicenow.streams.abstracts.IncrementalStream.sync")
    def test_non_permission_errors_still_propagate(self, mock_sync, *_):
        """Only permission errors are isolated; other failures stay fatal.

        IncrementalStream.sync already swallows non-permission ServiceNow errors
        internally and returns 0, so anything reaching this loop is either a
        permission failure or a genuine bug.
        """
        mock_sync.side_effect = ServiceNowNotFoundError("404 Not Found")

        with self.assertRaises(ServiceNowNotFoundError):
            sync(MagicMock(), {}, self._catalog("alpha", "beta"), {})

        # Aborted on the first stream rather than continuing.
        self.assertEqual(mock_sync.call_count, 1) 
