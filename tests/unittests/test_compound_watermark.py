"""
Unit tests for:
  - IncrementalStream.sync  — sys_updated_on bookmark with keyset pagination
  - BaseStream.get_records  — offset-based pagination driven by X-Total-Count
"""
import unittest
from unittest.mock import MagicMock, patch

import singer
from tap_servicenow.streams.abstracts import IncrementalStream, BaseStream


# ---------------------------------------------------------------------------
# Concrete minimal subclasses (abstract properties satisfied)
# ---------------------------------------------------------------------------

class ConcreteIncremental(IncrementalStream):
    tap_stream_id  = "test_stream"
    key_properties = ["sys_id"]
    replication_method = "INCREMENTAL"
    replication_keys   = ["sys_updated_on"]
    path = "test_stream"
    data_key = "result"


class ConcreteBase(BaseStream):
    tap_stream_id  = "base_stream"
    key_properties = ["sys_id"]
    replication_method = "FULL_TABLE"
    replication_keys   = []
    path = "base_stream"
    data_key = "result"

    def sync(self, state, transformer, parent_obj=None):
        pass  # not under test here


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_catalog(props=None):
    cat = MagicMock()
    cat.schema.to_dict.return_value = {
        "type": "object",
        "properties": props or {"sys_id": {}, "sys_updated_on": {}, "short_description": {}},
    }
    cat.metadata = []
    return cat


def _make_client(responses):
    """make_request returns successive dicts in order.

    get_total_count defaults to None so tests exercise the fallback path
    (stop on first empty page).  Override it per-test for count-driven tests.
    """
    c = MagicMock()
    c.base_url = "https://test.service-now.com/api/now/table"
    c.config = {"start_date": "2024-01-01T00:00:00Z"}
    c.make_request.side_effect = responses
    c.get_total_count.return_value = None
    return c


def _record(sys_id, updated_on, **extra):
    return {"sys_id": sys_id, "sys_updated_on": updated_on, **extra}


# ---------------------------------------------------------------------------
# IncrementalStream.sync — compound watermark
# ---------------------------------------------------------------------------

class TestIncrementalSync(unittest.TestCase):
    """Tests for IncrementalStream.sync() bookmark and query behaviour."""
    @patch("tap_servicenow.streams.abstracts.singer.write_state")
    @patch("tap_servicenow.streams.abstracts.write_record")
    @patch("tap_servicenow.streams.abstracts.write_bookmark")
    @patch("tap_servicenow.streams.abstracts.get_bookmark")
    def _run_sync(self, records_page, state, mock_get_bm, mock_write_bm,
                  mock_write_rec, mock_write_state, extra_pages=None):
        """Helper: wire up mocks, run one sync, return the stream + mocks."""
        pages = [{"result": records_page}]
        if extra_pages:
            pages += extra_pages
        pages.append({"result": []})  # terminal empty page

        mock_get_bm.return_value = state.get("dt_bookmark", "2024-01-01T00:00:00Z")
        mock_write_bm.side_effect = lambda s, st, k, v: s

        stream = ConcreteIncremental(_make_client(pages), _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.metadata.to_map", return_value={}):
            with singer.Transformer() as transformer:
                count = stream.sync(state={}, transformer=transformer)

        return stream, mock_write_bm, mock_write_rec, count

    def test_initial_sync_uses_gte_operator(self):
        """
        The query must always use sys_updated_on>= so the bookmark row is
        included.  There is no compound OR clause any more.
        """
        recs = [_record("id-1", "2024-06-01T10:00:00Z")]
        client = _make_client([{"result": recs}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record"):
                        with singer.Transformer() as t:
                            stream.sync(state={}, transformer=t)

        first_call_params = client.make_request.call_args_list[0][0][2]
        query = first_call_params["sysparm_query"]
        self.assertIn("sys_updated_on>=", query)
        self.assertNotIn("ORsys_updated_on=", query)  # no compound OR clause

    def test_subsequent_page_still_uses_gte_operator(self):
        """
        Even after processing the first page, subsequent pages still use
        sys_updated_on>= (no compound OR clause).
        """
        page1 = [_record(f"id-{i}", "2024-06-01T10:00:00Z") for i in range(5)]
        page2 = [_record("id-99", "2024-06-01T11:00:00Z")]
        client = _make_client([{"result": page1}, {"result": page2}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"
        stream.page_size = 5

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record"):
                        with singer.Transformer() as t:
                            stream.sync(state={}, transformer=t)

        second_call_params = client.make_request.call_args_list[1][0][2]
        query = second_call_params["sysparm_query"]
        self.assertIn("sys_updated_on>=", query)
        self.assertNotIn("ORsys_updated_on=", query)

    def test_orderby_both_fields(self):
        """Query must sort by sys_updated_on then sys_id."""
        page = [_record("id-1", "2024-06-01T10:00:00Z")]
        client = _make_client([{"result": page}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record"):
                        with singer.Transformer() as t:
                            stream.sync(state={}, transformer=t)

        params = client.make_request.call_args_list[0][0][2]
        query = params["sysparm_query"]
        self.assertIn("ORDERBYsys_updated_on", query)
        self.assertIn("ORDERBYsys_id", query)

    def test_only_sys_updated_on_bookmark_written(self):
        """write_bookmark must be called only for sys_updated_on (no sys_id_bookmark)."""
        page = [_record("id-A", "2024-06-01T10:00:00Z")]
        client = _make_client([{"result": page}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        written_keys = []
        def capture_write_bm(s, stream_name, key, value):
            written_keys.append(key)
            return s

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=capture_write_bm):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record"):
                        with singer.Transformer() as t:
                            stream.sync(state={}, transformer=t)

        self.assertIn("sys_updated_on", written_keys)
        self.assertNotIn("sys_id_bookmark", written_keys)

    def test_no_offset_param_in_sync_requests(self):
        """sysparm_offset must never appear in incremental sync requests."""
        page = [_record("id-1", "2024-06-01T10:00:00Z")]
        client = _make_client([{"result": page}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record"):
                        with singer.Transformer() as t:
                            stream.sync(state={}, transformer=t)

        for c in client.make_request.call_args_list:
            params = c[0][2] if len(c[0]) > 2 else {}
            self.assertNotIn("sysparm_offset", params)

    def test_performance_params_in_sync(self):
        """Every sync request must carry no_count and exclude_reference_link."""
        page = [_record("id-1", "2024-06-01T10:00:00Z")]
        client = _make_client([{"result": page}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record"):
                        with singer.Transformer() as t:
                            stream.sync(state={}, transformer=t)

        params = client.make_request.call_args_list[0][0][2]
        self.assertEqual(params.get("sysparm_no_count"), "true")
        self.assertEqual(params.get("sysparm_exclude_reference_link"), "true")

    def test_sysparm_fields_sent(self):
        """sysparm_fields must be included when the schema has properties."""
        props = {"sys_id": {}, "sys_updated_on": {}, "short_description": {}}
        page = [_record("id-1", "2024-06-01T10:00:00Z")]
        client = _make_client([{"result": page}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog(props))
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record"):
                        with singer.Transformer() as t:
                            stream.sync(state={}, transformer=t)

        params = client.make_request.call_args_list[0][0][2]
        fields_sent = set(params.get("sysparm_fields", "").split(","))
        self.assertTrue(fields_sent.issuperset({"sys_id", "sys_updated_on", "short_description"}))

    def test_empty_records_skipped_and_counted(self):
        """
        Empty dicts must be skipped (not emitted) and logged as a warning.
        is_selected() is mocked True so the counter reflects actual record
        processing, not stream-selection state.
        """
        page = [_record("id-1", "2024-06-01T10:00:00Z"), {}]
        client = _make_client([{"result": page}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        written = []
        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record", side_effect=lambda sid, r: written.append(r)):
                        with patch.object(stream, "is_selected", return_value=True):
                            with singer.Transformer() as t:
                                count = stream.sync(state={}, transformer=t)

        self.assertEqual(count, 1)  # only 1 non-empty record emitted
        self.assertFalse(any(r == {} for r in written))

    def test_raises_on_permission_exception(self):
        """sync must fail fast on permission errors (401/403)."""
        from tap_servicenow.exceptions import ServiceNowForbiddenError
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        # Simulate a 403 permission failure.
        client.make_request.side_effect = ServiceNowForbiddenError("403 Forbidden")

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with singer.Transformer() as t:
                        with self.assertRaises(ServiceNowForbiddenError) as ctx:
                            stream.sync(state={}, transformer=t)

        self.assertIn("Permission error while syncing stream 'test_stream'", str(ctx.exception))

    def test_permission_error_does_not_advance_bookmark(self):
        """A permission failure must NOT write a bookmark.

        This is the data-loss case the raise exists to prevent: the previous
        code broke out of the pagination loop and fell through to
        write_bookmark, saving a watermark that covered rows the tap never
        fetched. Those rows are then skipped forever on the next run.
        """
        from tap_servicenow.exceptions import ServiceNowForbiddenError
        client = _make_client([
            # First page succeeds, so current_max_dt advances in memory...
            {"result": [_record("id-1", "2024-02-01T00:00:00Z")]},
        ])
        # ...then the second page 403s part-way through the table.
        client.make_request.side_effect = [
            {"result": [_record("id-1", "2024-02-01T00:00:00Z")]},
            ServiceNowForbiddenError("403 Forbidden"),
        ]

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark") as mock_wb:
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with singer.Transformer() as t:
                        with self.assertRaises(ServiceNowForbiddenError):
                            stream.sync(state={}, transformer=t)

        mock_wb.assert_not_called()

    def _sync_capturing_bookmarks(self, pages):
        """Run sync over `pages` and return the bookmark values written."""
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        if isinstance(pages, list):
            client.make_request.side_effect = pages
        else:
            client.make_request.return_value = pages

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        written = []
        self._bookmarks_written = written
        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark",
                       side_effect=lambda s, st, k, v: written.append(v) or s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with singer.Transformer() as t:
                        stream.sync(state={}, transformer=t)
        return written

    def test_stalled_cursor_raises_and_does_not_advance_bookmark(self):
        """A page with readable rows that does not move the cursor is anomalous.

        sys_id is a unique primary key, so two consecutive pages ending on the
        same (sys_updated_on, sys_id) means ServiceNow served the same page
        twice - the signature of a query_range ACL stripping the range clauses
        and answering HTTP 200. Stopping is correct, but advancing the bookmark
        would skip every remaining row permanently on a run reporting success.
        """
        from tap_servicenow.exceptions import ServiceNowIncompleteSyncError
        with self.assertRaises(ServiceNowIncompleteSyncError) as ctx:
            self._sync_capturing_bookmarks(
                {"result": [_record("id-1", "2024-02-01T00:00:00Z")]}  # same page forever
            )
        self.assertIn("NOT fully replicated", str(ctx.exception))
        self.assertEqual(self._bookmarks_written, [],
                         "bookmark must be held back on a stalled cursor")

    def test_healthy_pagination_still_advances_bookmark(self):
        """The stall guard must not fire on a normal multi-page sync."""
        written = self._sync_capturing_bookmarks([
            {"result": [_record("id-1", "2024-02-01T00:00:00Z")]},
            {"result": [_record("id-2", "2024-02-02T00:00:00Z")]},
            {"result": []},                                   # normal end-of-data
        ])
        self.assertEqual(written, ["2024-02-02 00:00:00"])

    def test_fully_masked_page_is_a_stall_not_end_of_data(self):
        """A page of all-{} rows carries no cursor, so the sync is stranded.

        Only an EMPTY page means end-of-data. A page with rows on it does not,
        whatever those rows contain - field-level ACLs can mask every field of
        every row, and more pages still follow. Treating it as a clean finish
        advanced the bookmark past rows that were never fetched.
        """
        from tap_servicenow.exceptions import ServiceNowIncompleteSyncError
        with self.assertRaises(ServiceNowIncompleteSyncError):
            self._sync_capturing_bookmarks([
                {"result": [_record("id-1", "2024-02-01T00:00:00Z")]},
                {"result": [{}, {}]},                       # every row masked
                {"result": [_record("id-3", "2024-02-03T00:00:00Z")]},
                {"result": []},
            ])
        self.assertEqual(self._bookmarks_written, [])

    def test_returns_zero_on_non_permission_servicenow_error(self):
        """Non-permission ServiceNow errors should still be logged and skipped."""
        from tap_servicenow.exceptions import ServiceNowNotFoundError
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.make_request.side_effect = ServiceNowNotFoundError("404 Not Found")

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with singer.Transformer() as t:
                        count = stream.sync(state={}, transformer=t)

        self.assertEqual(count, 0)

    def test_page_size_default_is_1000(self):
        """page_size class attribute must be 1000, not 5000."""
        self.assertEqual(ConcreteIncremental.page_size, 1000)


# ---------------------------------------------------------------------------
# BaseStream.get_records — offset-based pagination
# ---------------------------------------------------------------------------

class TestGetRecordsOffset(unittest.TestCase):
    """Tests for BaseStream.get_records() — offset-based pagination.

    get_total_count is stubbed to None by _make_client so tests exercise the
    fallback path (stop on first empty page) unless overridden explicitly.
    """

    def _stream(self, pages):
        # Append the terminal empty page that signals end-of-data on the
        # fallback (no total_count) path.
        client = _make_client([{"result": p} for p in pages] + [{"result": []}])
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"
        return stream, client

    def test_offset_starts_at_zero(self):
        """First page request must include sysparm_offset=0."""
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        list(stream.get_records())
        params = client.make_request.call_args_list[0][0][2]
        self.assertEqual(params.get("sysparm_offset"), 0)

    def test_offset_advances_by_page_size(self):
        """Second page must use sysparm_offset == page_size."""
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(5)]
        page2 = [_record("id-99", "2024-01-02T00:00:00Z")]
        stream, client = self._stream([page1, page2])
        stream.page_size = 5
        result = list(stream.get_records())
        self.assertEqual(len(result), 6)
        second_params = client.make_request.call_args_list[1][0][2]
        self.assertEqual(second_params.get("sysparm_offset"), 5)

    def test_offset_present_on_all_pages(self):
        """Every data request must carry a sysparm_offset parameter."""
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(3)]
        page2 = [_record("id-end", "2024-01-02T00:00:00Z")]
        stream, client = self._stream([page1, page2])
        stream.page_size = 3
        list(stream.get_records())
        for call in client.make_request.call_args_list:
            params = call[0][2] if len(call[0]) > 2 else {}
            self.assertIn("sysparm_offset", params)

    def test_performance_params_always_present(self):
        """Every request must carry no_count and exclude_reference_link."""
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        list(stream.get_records())
        params = client.make_request.call_args_list[0][0][2]
        self.assertEqual(params.get("sysparm_no_count"), "true")
        self.assertEqual(params.get("sysparm_exclude_reference_link"), "true")

    def test_sysparm_fields_from_schema(self):
        """sysparm_fields must include every key from the stream schema."""
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        list(stream.get_records())
        params = client.make_request.call_args_list[0][0][2]
        schema_keys = set(stream.schema.get("properties", {}).keys())
        sent_fields = set(params.get("sysparm_fields", "").split(","))
        self.assertTrue(sent_fields.issuperset(schema_keys - {""}))

    def test_empty_records_skipped_by_get_records(self):
        """Empty {} dicts in the result array must not be yielded."""
        rows = [_record("id-1", "2024-01-01T00:00:00Z"), {}]
        stream, client = self._stream([rows])
        result = list(stream.get_records())
        self.assertNotIn({}, result)
        self.assertEqual(len(result), 1)

    def test_short_page_does_not_stop(self):
        """
        ServiceNow returns short pages when row-level ACLs filter rows
        post-query.  A short page (< page_size) must NOT end pagination.
        """
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(3)]  # < page_size
        page2 = [_record("id-end", "2024-01-02T00:00:00Z")]
        stream, client = self._stream([page1, page2])
        stream.page_size = 5
        result = list(stream.get_records())
        self.assertEqual(len(result), 4)

    def test_stops_on_empty_page_when_no_total_count(self):
        """Fallback: stop on first empty page when X-Total-Count is unavailable."""
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(5)]
        stream, client = self._stream([page1])  # _stream appends the terminal empty page
        stream.page_size = 5
        list(stream.get_records())
        # One data page + the terminal empty page = 2 requests.
        self.assertEqual(client.make_request.call_count, 2)

    def test_stops_at_total_count(self):
        """When get_total_count returns N, pagination stops once offset >= N."""
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        # Table has 3 records; page_size=2 → 2 data pages, no empty-page probe.
        client.get_total_count.return_value = 3
        client.make_request.side_effect = [
            {"result": [_record("id-1", "2024-01-01T00:00:00Z"),
                        _record("id-2", "2024-01-02T00:00:00Z")]},
            {"result": [_record("id-3", "2024-01-03T00:00:00Z")]},
        ]
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"
        stream.page_size = 2

        result = list(stream.get_records())
        self.assertEqual(len(result), 3)
        # offset=0, offset=2 → exactly 2 data requests; no extra empty-page probe.
        self.assertEqual(client.make_request.call_count, 2)

    def test_continues_through_empty_page_when_total_count_known(self):
        """
        When X-Total-Count is available, an empty mid-table page caused by
        ACL-hidden rows must NOT stop pagination.  Records at higher offsets
        must still be retrieved.
        """
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        # Table has 300 rows; only rows at offset 0 and 200 are accessible.
        client.get_total_count.return_value = 300
        client.make_request.side_effect = [
            {"result": [_record("id-1", "2024-01-01T00:00:00Z")]},  # offset=0
            {"result": []},                                           # offset=100 (ACL hidden)
            {"result": [_record("id-2", "2024-01-02T00:00:00Z")]},  # offset=200
        ]
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"
        stream.page_size = 100

        result = list(stream.get_records())
        # All 3 pages are visited; both accessible records are returned.
        self.assertEqual(len(result), 2)
        self.assertEqual(client.make_request.call_count, 3)

    def test_total_count_probe_called_once(self):
        """get_total_count must be called exactly once per get_records() call."""
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        list(stream.get_records())
        client.get_total_count.assert_called_once()


# ---------------------------------------------------------------------------
# BaseStream.selected_fields — catalog field selection drives sysparm_fields
# ---------------------------------------------------------------------------

class TestSelectedFields(unittest.TestCase):
    """A deselected field must not be requested from ServiceNow; keys/replication
    keys are always retained; unspecified fields default to selected."""

    def _base(self, props, mdata):
        s = ConcreteBase(MagicMock(), None)
        s.schema = {"type": "object", "properties": {p: {} for p in props}}
        s.metadata = mdata
        return s

    def test_deselected_and_unsupported_fields_excluded(self):
        mdata = {
            ("properties", "sys_id"): {"inclusion": "automatic"},
            ("properties", "a"): {"inclusion": "available", "selected": True},
            ("properties", "b"): {"inclusion": "available", "selected": False},
            ("properties", "c"): {"inclusion": "unsupported"},
        }
        fields = self._base(["sys_id", "a", "b", "c"], mdata).selected_fields().split(",")
        self.assertIn("a", fields)
        self.assertIn("sys_id", fields)
        self.assertNotIn("b", fields)   # explicitly deselected -> not fetched
        self.assertNotIn("c", fields)   # unsupported -> not fetched

    def test_key_property_always_kept(self):
        mdata = {("properties", "sys_id"): {"inclusion": "available", "selected": False}}
        fields = self._base(["sys_id", "a"], mdata).selected_fields().split(",")
        self.assertIn("sys_id", fields)   # key retained for keyset pagination

    def test_replication_key_always_kept(self):
        s = ConcreteIncremental(MagicMock(), None)
        s.schema = {"type": "object", "properties": {"sys_id": {}, "sys_updated_on": {}, "a": {}}}
        s.metadata = {("properties", "sys_updated_on"): {"inclusion": "available", "selected": False}}
        self.assertIn("sys_updated_on", s.selected_fields().split(","))  # bookmark field retained

    def test_no_metadata_selects_all(self):
        fields = self._base(["sys_id", "a", "b"], {}).selected_fields().split(",")
        self.assertEqual(set(fields), {"sys_id", "a", "b"})


# ---------------------------------------------------------------------------
# BaseStream.get_records — permission errors (FULL_TABLE path)
# ---------------------------------------------------------------------------

class TestGetRecordsPermissionErrors(unittest.TestCase):
    """FullTableStream.sync has no error handling of its own, so get_records
    is the only place a permission failure can be classified on that path."""

    def _stream(self, side_effect):
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.make_request.side_effect = side_effect
        client.get_total_count.return_value = None  # fallback: stop on empty page
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"
        return stream

    def test_get_records_raises_on_forbidden(self):
        from tap_servicenow.exceptions import ServiceNowForbiddenError
        stream = self._stream(ServiceNowForbiddenError("403 Forbidden"))

        with self.assertRaises(ServiceNowForbiddenError) as ctx:
            list(stream.get_records())

        self.assertIn("Permission error while syncing stream 'base_stream'", str(ctx.exception))

    def test_get_records_raises_on_unauthorized(self):
        """A 401 must stay a 401, not be reclassified as a 403."""
        from tap_servicenow.exceptions import ServiceNowUnauthorizedError
        stream = self._stream(ServiceNowUnauthorizedError("401 Unauthorized"))

        with self.assertRaises(ServiceNowUnauthorizedError) as ctx:
            list(stream.get_records())

        self.assertIn("Permission error while syncing stream 'base_stream'", str(ctx.exception))

    def test_repeated_page_raises_instead_of_looping_forever(self):
        """A server that ignores sysparm_offset must not spin the loop forever.

        Without X-Total-Count the only stop condition is an empty page, so a
        server that keeps returning the same non-empty page never terminates.
        That is what a query_range ACL denial looks like: HTTP 200 with the
        pagination clause silently dropped. It has to raise - FullTableStream
        just drains this generator, so returning normally would report a
        truncated table as a complete one.
        """
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.get_total_count.return_value = None
        # Same page forever, regardless of offset.
        client.make_request.return_value = {
            "result": [_record("id-1", "2024-01-01T00:00:00Z")]
        }
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"

        from tap_servicenow.exceptions import ServiceNowIncompleteSyncError
        with self.assertRaises(ServiceNowIncompleteSyncError) as ctx:
            list(stream.get_records())
        self.assertIn("NOT fully replicated", str(ctx.exception))

    def test_repeated_page_is_not_emitted_twice(self):
        """The duplicate page must be detected before anything is yielded."""
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.get_total_count.return_value = None
        client.make_request.return_value = {
            "result": [_record("id-1", "2024-01-01T00:00:00Z")]
        }
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"

        from tap_servicenow.exceptions import ServiceNowIncompleteSyncError
        emitted = []
        with self.assertRaises(ServiceNowIncompleteSyncError):
            for r in stream.get_records():
                emitted.append(r)
        self.assertEqual(len(emitted), 1, "the repeated page was emitted twice")

    def test_no_stall_warning_on_healthy_pagination(self):
        """Healthy multi-page pagination completes without raising."""
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.get_total_count.return_value = None  # fallback: stop on empty page
        client.make_request.side_effect = [
            {"result": [_record("id-1", "2024-01-01T00:00:00Z")]},
            {"result": [_record("id-2", "2024-01-02T00:00:00Z")]},
            {"result": []},
        ]
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"

        records = list(stream.get_records())
        self.assertEqual(len(records), 2)   # completes normally, no raise

    def test_error_names_real_endpoint_when_url_endpoint_unset(self):
        """get_records is callable before sync sets url_endpoint.

        make_request falls back to base_url/path in that case, so the error must
        name the URL the request actually went to rather than an empty string.
        """
        from tap_servicenow.exceptions import ServiceNowForbiddenError
        stream = self._stream(ServiceNowForbiddenError("403 Forbidden"))
        stream.url_endpoint = ""      # not yet set by sync()

        with self.assertRaises(ServiceNowForbiddenError) as ctx:
            list(stream.get_records())

        self.assertIn(
            "https://test.service-now.com/api/now/table/base_stream",
            str(ctx.exception),
        )
        self.assertNotIn("endpoint ''", str(ctx.exception))

    def test_get_records_does_not_yield_partial_page_on_forbidden(self):
        """A mid-table 403 must not silently return the rows gathered so far.

        The previous behaviour set has_more = False and returned a truncated
        record set with no error signal anywhere.
        """
        from tap_servicenow.exceptions import ServiceNowForbiddenError
        stream = self._stream([
            {"result": [_record("id-1", "2024-01-01T00:00:00Z")]},
            ServiceNowForbiddenError("403 Forbidden"),
        ])

        collected = []
        with self.assertRaises(ServiceNowForbiddenError):
            for record in stream.get_records():
                collected.append(record)

        # The first page's row is yielded before the failure, but the consumer
        # sees the exception rather than a clean end-of-stream.
        self.assertEqual(len(collected), 1)


if __name__ == "__main__":
    unittest.main()


class TestUncursorableRecords(unittest.TestCase):
    """Records missing the replication key or sys_id cannot position the cursor.

    The old code substituted the bookmark for a missing sys_updated_on, which
    drove the cursor BACKWARDS: the next query rewound to the start of the
    range and re-served the same page, so the stream never advanced and re-read
    the same rows on every future run. Field-level ACLs make this reachable -
    ServiceNow answers with HTTP 200 and the field simply omitted.
    """

    def _sync(self, pages):
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.make_request.side_effect = pages
        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"
        written = []
        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark",
                       side_effect=lambda s, st, k, v: written.append(v) or s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with singer.Transformer() as t:
                        count = stream.sync(state={}, transformer=t)
        return client, written, count

    def test_record_without_replication_key_does_not_rewind_cursor(self):
        """A blank sys_updated_on must be skipped, not substituted."""
        client, written, _ = self._sync([
            {"result": [
                _record("id-1", "2024-02-01T00:00:00Z"),
                {"sys_id": "id-2", "sys_updated_on": ""},   # unreadable field
            ]},
            {"result": []},
        ])
        # The cursor must sit on id-1, NOT be rewound to the bookmark.
        second_query = client.make_request.call_args_list[1][0][2]["sysparm_query"]
        self.assertIn("2024-02-01 00:00:00", second_query)
        self.assertEqual(written, ["2024-02-01 00:00:00"])

    def test_record_without_sys_id_is_skipped(self):
        client, written, _ = self._sync([
            {"result": [
                _record("id-1", "2024-02-01T00:00:00Z"),
                {"sys_updated_on": "2024-02-02T00:00:00Z"},   # no sys_id
            ]},
            {"result": []},
        ])
        self.assertEqual(written, ["2024-02-01 00:00:00"])

    def test_page_of_only_uncursorable_records_is_a_stall(self):
        """If nothing on the page can position the cursor, we are stranded."""
        from tap_servicenow.exceptions import ServiceNowIncompleteSyncError
        with self.assertRaises(ServiceNowIncompleteSyncError):
            self._sync([
                {"result": [_record("id-1", "2024-02-01T00:00:00Z")]},
                {"result": [{"sys_id": "id-2", "sys_updated_on": ""}]},
                {"result": []},
            ])


class TestStalledPageNotReEmitted(unittest.TestCase):
    """get_records must check a page for repetition before emitting it.

    On a stall the same rows arrive twice. Yielding as they were read put the
    duplicate page downstream before the stall was detected. The stall now
    comes from a server that ignores sysparm_offset rather than from a keyset
    cursor, but the requirement is unchanged.
    """

    def test_stalled_page_is_not_emitted(self):
        from tap_servicenow.exceptions import ServiceNowIncompleteSyncError
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        # No X-Total-Count, so the loop's only stop signal is an empty page.
        client.get_total_count.return_value = None
        # Same single row forever: page 1 is emitted, page 2 repeats it and
        # must be suppressed.
        client.make_request.return_value = {
            "result": [_record("id-1", "2024-01-01T00:00:00Z")]
        }
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"

        collected = []
        with self.assertRaises(ServiceNowIncompleteSyncError):
            for rec in stream.get_records():
                collected.append(rec["sys_id"])

        self.assertEqual(
            collected, ["id-1"],
            "the repeated page must not be emitted a second time",
        )
