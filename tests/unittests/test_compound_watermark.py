"""
Unit tests for:
  - IncrementalStream.sync  — compound watermark (sys_updated_on + sys_id)
  - BaseStream.get_records  — keyset pagination (sys_id-based, no sysparm_offset)
"""
import unittest
from unittest.mock import MagicMock, patch, call

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

class TestIncrementalSyncCompoundWatermark(unittest.TestCase):

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

        mock_get_bm.side_effect = lambda s, st, key=None, default=None: (
            state.get("sys_id_bookmark", "") if key == "sys_id_bookmark" else state.get("dt_bookmark", "2024-01-01T00:00:00Z")
        )
        mock_write_bm.side_effect = lambda s, st, k, v: s

        stream = ConcreteIncremental(_make_client(pages), _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        with patch("tap_servicenow.streams.abstracts.metadata.to_map", return_value={}):
            with singer.Transformer() as transformer:
                count = stream.sync(state={}, transformer=transformer)

        return stream, mock_write_bm, mock_write_rec, count

    def test_initial_sync_uses_gte_operator(self):
        """
        On the very first page when there is no sys_id bookmark (empty string)
        the query must use sys_updated_on>= so the bookmark row itself is
        included.  A key-aware side_effect distinguishes the two get_bookmark
        calls (replication key vs sys_id_bookmark).
        """
        recs = [_record("id-1", "2024-06-01T10:00:00Z")]
        client = _make_client([{"result": recs}, {"result": []}])
        client.config = {"start_date": "2024-01-01T00:00:00Z"}

        stream = ConcreteIncremental(client, _make_catalog())
        stream.url_endpoint = "https://test.service-now.com/api/now/table/test_stream"

        # Return empty string only for the sys_id_bookmark key so last_page_sid=""
        def bm_side_effect(state, stream_name, key=None, default=None):
            if key == "sys_id_bookmark":
                return ""   # no prior sys_id bookmark → triggers >= path
            return "2024-01-01T00:00:00Z"

        with patch("tap_servicenow.streams.abstracts.get_bookmark", side_effect=bm_side_effect):
            with patch("tap_servicenow.streams.abstracts.write_bookmark", side_effect=lambda s, st, k, v: s):
                with patch("tap_servicenow.streams.abstracts.singer.write_state"):
                    with patch("tap_servicenow.streams.abstracts.write_record"):
                        with singer.Transformer() as t:
                            stream.sync(state={}, transformer=t)

        first_call_params = client.make_request.call_args_list[0][0][2]
        query = first_call_params["sysparm_query"]
        self.assertIn("sys_updated_on>=", query)

    def test_subsequent_page_uses_compound_or_clause(self):
        """
        After the first record is seen, subsequent pages must use the compound
        (sys_updated_on>X)^OR(sys_updated_on=X^sys_id>Y)^ORDERBY... query.
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
        # Compound or clause must be present
        self.assertIn("ORsys_updated_on=", query)
        self.assertIn("sys_id>", query)

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

    def test_both_bookmark_fields_written(self):
        """write_bookmark must be called for sys_updated_on AND sys_id_bookmark."""
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
        self.assertIn("sys_id_bookmark", written_keys)

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

    def test_returns_zero_on_exception(self):
        """sync must catch unhandled exceptions, log critical, and return 0."""
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.make_request.side_effect = RuntimeError("boom")

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
        client = _make_client([{"result": p} for p in pages])
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

    def test_stops_on_partial_page(self):
        page1 = [_record(f"id-{i}", "2024-01-01T00:00:00Z") for i in range(5)]
        page2 = [_record("id-end", "2024-01-02T00:00:00Z")]  # partial
        stream, client = self._stream([page1, page2])
        stream.page_size = 5
        list(stream.get_records())
        self.assertEqual(client.make_request.call_count, 2)

    def test_orderby_sys_id_in_query(self):
        rows = [_record("id-1", "2024-01-01T00:00:00Z")]
        stream, client = self._stream([rows])
        list(stream.get_records())
        params = client.make_request.call_args_list[0][0][2]
        self.assertIn("ORDERBYsys_id", params["sysparm_query"])


if __name__ == "__main__":
    unittest.main()
