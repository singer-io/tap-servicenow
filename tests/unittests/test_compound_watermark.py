"""
Unit tests for:
  - IncrementalStream.sync  — sys_updated_on bookmark with keyset pagination
  - BaseStream.get_records  — keyset pagination (sys_id-based, no sysparm_offset)
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
    """make_request returns successive dicts in order."""
    c = MagicMock()
    c.base_url = "https://test.service-now.com/api/now/table"
    c.config = {"start_date": "2024-01-01T00:00:00Z"}
    c.make_request.side_effect = responses
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
        with patch("tap_servicenow.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            with patch("tap_servicenow.streams.abstracts.write_bookmark",
                       side_effect=lambda s, st, k, v: written.append(v) or s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with singer.Transformer() as t:
                        stream.sync(state={}, transformer=t)
        return written

    def test_stalled_cursor_does_not_advance_bookmark(self):
        """A page with readable rows that does not move the cursor is anomalous.

        sys_id is a unique primary key, so two consecutive pages ending on the
        same (sys_updated_on, sys_id) means ServiceNow served the same page
        twice - the signature of a query_range ACL stripping the range clauses
        and answering HTTP 200. Stopping is correct, but advancing the bookmark
        would skip every remaining row permanently on a run reporting success.
        """
        written = self._sync_capturing_bookmarks(
            {"result": [_record("id-1", "2024-02-01T00:00:00Z")]}   # same page forever
        )
        self.assertEqual(written, [], "bookmark must be held back on a stalled cursor")

    def test_healthy_pagination_still_advances_bookmark(self):
        """The stall guard must not fire on a normal multi-page sync."""
        written = self._sync_capturing_bookmarks([
            {"result": [_record("id-1", "2024-02-01T00:00:00Z")]},
            {"result": [_record("id-2", "2024-02-02T00:00:00Z")]},
            {"result": []},                                   # normal end-of-data
        ])
        self.assertEqual(written, ["2024-02-02 00:00:00"])

    def test_all_empty_page_is_not_treated_as_a_stall(self):
        """Rows hidden by row-level ACLs come back as {} and carry no cursor.

        That is the ordinary ACL case the guard was built for, already reported
        via empty_record_count - it must not be escalated to the stall path.
        """
        written = self._sync_capturing_bookmarks([
            {"result": [{}, {}]},
            {"result": []},
        ])
        self.assertEqual(written, ["2024-01-01 00:00:00"])

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
# BaseStream.get_records — keyset pagination
# ---------------------------------------------------------------------------

class TestGetRecordsKeyset(unittest.TestCase):

    def _stream(self, pages):
        # Trailing empty page: keyset pagination past the end of a table returns
        # an empty result set, which is the correct stop signal. A short page is
        # not, because row-level ACLs shrink pages after the query runs.
        client = _make_client([{"result": p} for p in pages] + [{"result": []}])
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"
        return stream, client

    def test_single_page_no_offset(self):
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        result = list(stream.get_records())
        self.assertEqual(len(result), 1)
        # No sysparm_offset in the call
        params = client.make_request.call_args_list[0][0][2]
        self.assertNotIn("sysparm_offset", params)

    def test_multi_page_advances_sys_id_cursor(self):
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(5)]
        page2 = [_record("id-99", "2024-01-02T00:00:00Z")]
        stream, client = self._stream([page1, page2])
        stream.page_size = 5
        result = list(stream.get_records())
        self.assertEqual(len(result), 6)

        second_params = client.make_request.call_args_list[1][0][2]
        # Last sys_id from page1 is "id-4"
        self.assertIn("sys_id>id-4", second_params["sysparm_query"])

    def test_no_offset_across_all_pages(self):
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(3)]
        page2 = [_record("id-end", "2024-01-02T00:00:00Z")]
        stream, client = self._stream([page1, page2])
        stream.page_size = 3
        list(stream.get_records())
        for c in client.make_request.call_args_list:
            params = c[0][2]
            self.assertNotIn("sysparm_offset", params)

    def test_performance_params_always_present(self):
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        list(stream.get_records())
        params = client.make_request.call_args_list[0][0][2]
        self.assertEqual(params.get("sysparm_no_count"), "true")
        self.assertEqual(params.get("sysparm_exclude_reference_link"), "true")

    def test_sysparm_fields_from_schema(self):
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        list(stream.get_records())
        params = client.make_request.call_args_list[0][0][2]
        schema_keys = set(stream.schema.get("properties", {}).keys())
        sent_fields = set(params.get("sysparm_fields", "").split(","))
        self.assertTrue(sent_fields.issuperset(schema_keys - {""}))

    def test_empty_records_skipped_by_get_records(self):
        rows = [_record("id-1", "2024-01-01T00:00:00Z"), {}]
        stream, client = self._stream([rows])
        result = list(stream.get_records())
        self.assertNotIn({}, result)
        self.assertEqual(len(result), 1)

    def test_short_page_does_not_stop(self):
        """
        Regression: ServiceNow returns short pages when row-level ACLs filter
        rows post-query. A short page must NOT end pagination or the table is
        silently truncated. Both pages here are shorter than page_size, yet all
        rows must be returned.
        """
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(3)]  # < page_size
        page2 = [_record("id-end", "2024-01-02T00:00:00Z")]
        stream, client = self._stream([page1, page2])
        stream.page_size = 5
        result = list(stream.get_records())
        self.assertEqual(len(result), 4)

    def test_stops_on_empty_page(self):
        """get_records stops on the first empty page, not on a short one."""
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(5)]
        stream, client = self._stream([page1])   # _stream appends the empty page
        stream.page_size = 5
        list(stream.get_records())
        # One data page + the terminal empty page.
        self.assertEqual(client.make_request.call_count, 2)

    def test_orderby_sys_id_in_query(self):
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        list(stream.get_records())
        params = client.make_request.call_args_list[0][0][2]
        self.assertIn("ORDERBYsys_id", params["sysparm_query"])


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

    def test_stalled_cursor_logs_critical(self):
        """get_records has no bookmark to hold back, so it must at least shout.

        FullTableStream.sync consumes this generator and would otherwise treat a
        stuck cursor as a completed table.
        """
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        # Same readable row every page: cursor can never advance.
        client.make_request.return_value = {"result": [_record("id-1", "2024-01-01T00:00:00Z")]}
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"

        with patch("tap_servicenow.streams.abstracts.LOGGER") as mock_log:
            records = list(stream.get_records())

        # Page 1 legitimately advances the cursor off its empty initial value;
        # page 2 returns the same row, which is where the stall is detected. Two
        # records rather than an infinite stream is the point - the duplicate is
        # harmless because targets upsert on the primary key.
        self.assertEqual(len(records), 2)
        mock_log.critical.assert_called_once()
        self.assertIn("INCOMPLETE", mock_log.critical.call_args[0][0])

    def test_no_stall_warning_on_healthy_pagination(self):
        """The guard must stay silent when the cursor advances normally."""
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.make_request.side_effect = [
            {"result": [_record("id-1", "2024-01-01T00:00:00Z")]},
            {"result": [_record("id-2", "2024-01-02T00:00:00Z")]},
            {"result": []},
        ]
        stream = ConcreteBase(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/base_stream"

        with patch("tap_servicenow.streams.abstracts.LOGGER") as mock_log:
            records = list(stream.get_records())

        self.assertEqual(len(records), 2)
        mock_log.critical.assert_not_called()

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
