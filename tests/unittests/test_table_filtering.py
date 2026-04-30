"""
Unit tests for:
  - tap_servicenow.streams.DEFAULT_EXCLUDED_TABLES
  - tap_servicenow.streams.get_all_tables  (keyset pagination)
  - tap_servicenow.streams.get_sync_tables (filtering logic)
"""
import unittest
from unittest.mock import MagicMock, patch, call

from tap_servicenow.streams import (
    DEFAULT_EXCLUDED_TABLES,
    get_all_tables,
    get_sync_tables,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(pages):
    """
    Build a mock Client whose make_request returns successive pages.
    Each item in *pages* is the list of records for that page.
    """
    client = MagicMock()
    client.base_url = "https://test.service-now.com/api/now/table"
    client.make_request.side_effect = [{"result": p} for p in pages]
    return client


def _row(name, sys_id, super_class=""):
    return {"name": name, "sys_id": sys_id, "super_class.name": super_class}


# ---------------------------------------------------------------------------
# DEFAULT_EXCLUDED_TABLES
# ---------------------------------------------------------------------------

class TestDefaultExcludedTables(unittest.TestCase):

    EXPECTED_TABLES = {
        "sys_audit",
        "sys_audit_delete",
        "syslog",
        "syslog_transaction",
        "sys_email_log",
        "sys_history_line",
        "sys_history_set",
        "ha_log",
        "sys_cache_flush",
        "sys_cluster_state",
    }

    def test_all_required_tables_present(self):
        for tbl in self.EXPECTED_TABLES:
            with self.subTest(table=tbl):
                self.assertIn(tbl, DEFAULT_EXCLUDED_TABLES)

    def test_is_frozenset(self):
        self.assertIsInstance(DEFAULT_EXCLUDED_TABLES, frozenset)

    def test_contains_exactly_ten_entries(self):
        self.assertEqual(len(DEFAULT_EXCLUDED_TABLES), 10)


# ---------------------------------------------------------------------------
# get_all_tables — keyset pagination behaviour
# ---------------------------------------------------------------------------

class TestGetAllTablesKeyset(unittest.TestCase):

    def test_single_page_returns_all_tables(self):
        """When page has fewer rows than limit, only one request is made."""
        page = [
            _row("incident", "aaa-1"),
            _row("problem", "aaa-2"),
        ]
        client = _make_client([page])
        result = get_all_tables(client, page_size=500)

        self.assertEqual(result, {"incident": "", "problem": ""})
        self.assertEqual(client.make_request.call_count, 1)

    def test_multi_page_uses_sys_id_cursor(self):
        """
        Second page must receive a query containing the last sys_id seen on
        the first page — proving keyset (not offset) pagination is in use.
        """
        page1 = [_row("incident", "id-1"), _row("problem", "id-2")]
        page2 = [_row("change_request", "id-3")]
        client = _make_client([page1, page2])

        result = get_all_tables(client, page_size=2)

        self.assertIn("incident", result)
        self.assertIn("problem", result)
        self.assertIn("change_request", result)

        # First call: no sys_id cursor yet → query starts with ORDERBY
        first_params = client.make_request.call_args_list[0][1]["params"]
        self.assertIn("ORDERBYsys_id", first_params["sysparm_query"])
        self.assertNotIn("sys_id>", first_params["sysparm_query"])

        # Second call: cursor from last row of page1 (sys_id = "id-2")
        second_params = client.make_request.call_args_list[1][1]["params"]
        self.assertIn("sys_id>id-2", second_params["sysparm_query"])

    def test_no_offset_param_in_any_request(self):
        """sysparm_offset must never appear — we use keyset pagination only."""
        page = [_row("incident", "id-1")]
        client = _make_client([page])
        get_all_tables(client, page_size=500)

        for c in client.make_request.call_args_list:
            params = c[1].get("params", {})
            self.assertNotIn("sysparm_offset", params)

    def test_performance_params_always_present(self):
        """Every request must include sysparm_no_count and sysparm_exclude_reference_link."""
        page = [_row("incident", "id-1")]
        client = _make_client([page])
        get_all_tables(client, page_size=500)

        params = client.make_request.call_args_list[0][1]["params"]
        self.assertEqual(params.get("sysparm_no_count"), "true")
        self.assertEqual(params.get("sysparm_exclude_reference_link"), "true")

    def test_empty_response_returns_empty_dict(self):
        client = _make_client([[]])
        result = get_all_tables(client)
        self.assertEqual(result, {})

    def test_super_class_captured(self):
        """The dict value should hold the parent table name."""
        page = [_row("incident", "id-1", super_class="task")]
        client = _make_client([page])
        result = get_all_tables(client, page_size=500)
        self.assertEqual(result.get("incident"), "task")

    def test_super_class_missing_defaults_to_empty_string(self):
        page = [{"name": "sys_user", "sys_id": "id-99"}]  # no super_class key
        client = _make_client([page])
        result = get_all_tables(client, page_size=500)
        self.assertEqual(result.get("sys_user"), "")

    def test_super_class_as_nested_dict_display_value(self):
        """super_class arriving as a nested dict should be unwrapped."""
        page = [{"name": "incident", "sys_id": "id-1",
                 "super_class": {"display_value": "task", "value": "task-sys-id"}}]
        client = _make_client([page])
        result = get_all_tables(client, page_size=500)
        self.assertEqual(result.get("incident"), "task")

    def test_rows_with_no_name_are_ignored(self):
        page = [{"sys_id": "id-orphan"}]  # no 'name' key
        client = _make_client([page])
        result = get_all_tables(client, page_size=500)
        self.assertEqual(result, {})

    def test_stops_when_page_smaller_than_limit(self):
        """Only two make_request calls for two pages, third call must not happen."""
        page1 = [_row(f"t{i}", f"id-{i}") for i in range(5)]
        page2 = [_row("last", "id-99")]   # < page_size=5 → stop
        client = _make_client([page1, page2])

        get_all_tables(client, page_size=5)
        self.assertEqual(client.make_request.call_count, 2)


# ---------------------------------------------------------------------------
# get_sync_tables — filtering logic
# ---------------------------------------------------------------------------

class TestGetSyncTables(unittest.TestCase):

    BASE_MAP = {
        "incident": "task",
        "problem": "",
        "sys_audit": "",
        "syslog": "",
        "sys_history_line": "",
        "my_custom_table": "",
    }

    def test_default_excluded_tables_removed(self):
        result = get_sync_tables(self.BASE_MAP)
        for excluded in DEFAULT_EXCLUDED_TABLES:
            self.assertNotIn(excluded, result)

    def test_non_excluded_tables_present(self):
        result = get_sync_tables(self.BASE_MAP)
        self.assertIn("incident", result)
        self.assertIn("problem", result)
        self.assertIn("my_custom_table", result)

    def test_include_tables_acts_as_allowlist(self):
        config = {"include_tables": ["incident"]}
        result = get_sync_tables(self.BASE_MAP, config)
        self.assertEqual(result, ["incident"])

    def test_include_tables_empty_list_returns_all_non_excluded(self):
        config = {"include_tables": []}
        result = get_sync_tables(self.BASE_MAP, config)
        self.assertIn("incident", result)
        self.assertIn("problem", result)

    def test_exclude_tables_config_additive(self):
        config = {"exclude_tables": ["my_custom_table"]}
        result = get_sync_tables(self.BASE_MAP, config)
        self.assertNotIn("my_custom_table", result)
        self.assertIn("incident", result)

    def test_exclude_tables_does_not_affect_non_listed(self):
        config = {"exclude_tables": ["my_custom_table"]}
        result = get_sync_tables(self.BASE_MAP, config)
        self.assertIn("problem", result)

    def test_include_and_exclude_interaction(self):
        """include_tables is the allowlist; exclude_tables further removes entries."""
        config = {
            "include_tables": ["incident", "problem"],
            "exclude_tables": ["problem"],
        }
        result = get_sync_tables(self.BASE_MAP, config)
        self.assertEqual(result, ["incident"])

    def test_empty_table_map_returns_empty_list(self):
        result = get_sync_tables({})
        self.assertEqual(result, [])

    def test_all_tables_excluded_returns_empty_list(self):
        only_excluded = {t: "" for t in DEFAULT_EXCLUDED_TABLES}
        result = get_sync_tables(only_excluded)
        self.assertEqual(result, [])

    def test_none_config_treated_as_empty(self):
        result = get_sync_tables(self.BASE_MAP, None)
        self.assertIn("incident", result)

    def test_default_excluded_tables_cannot_be_re_added_via_exclude(self):
        """Passing a default-excluded table in exclude_tables is a no-op (already excluded)."""
        config = {"exclude_tables": ["sys_audit"]}
        result = get_sync_tables(self.BASE_MAP, config)
        self.assertNotIn("sys_audit", result)


if __name__ == "__main__":
    unittest.main()
