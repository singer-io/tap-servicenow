"""
Unit tests for:
  - tap_servicenow.schema.get_dynamic_schema
      - table filtering (DEFAULT_EXCLUDED_TABLES applied)
      - batch sys_dictionary queries (nameIN operator)
      - table inheritance resolution (super_class chain walk)
      - deferred 401/403 unauthorised-table summary logging
      - performance params on access-check requests
"""
import unittest
from unittest.mock import MagicMock, patch, call

from tap_servicenow.schema import get_dynamic_schema
from tap_servicenow.streams import DEFAULT_EXCLUDED_TABLES
from tap_servicenow.exceptions import ServiceNowForbiddenError, ServiceNowUnauthorizedError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(config=None):
    c = MagicMock()
    c.base_url = "https://test.service-now.com/api/now/table"
    c.config = config or {}
    # Default: tables are accessible — get() returns None (no exception)
    c.get.return_value = None
    return c


def _dict_fields(table, *elements):
    """Build a list of sys_dictionary field records for *table*."""
    return [
        {"name": table, "element": elem, "internal_type": "string"}
        for elem in elements
    ]


def _db_row(name, sys_id, super_class=""):
    return {"name": name, "sys_id": sys_id, "super_class.name": super_class}


# ---------------------------------------------------------------------------
# Minimal fake for get_all_tables + get_sync_tables so we control inputs
# ---------------------------------------------------------------------------

def _patch_tables(table_map, sync_list=None):
    """
    Patch get_all_tables to return *table_map* and get_sync_tables to return
    *sync_list* (defaults to table_map keys minus excluded tables).
    """
    if sync_list is None:
        sync_list = [k for k in table_map if k not in DEFAULT_EXCLUDED_TABLES]
    return (
        patch("tap_servicenow.schema.get_all_tables", return_value=table_map),
        patch("tap_servicenow.schema.get_sync_tables", return_value=sync_list),
    )


# ---------------------------------------------------------------------------
# Table filtering
# ---------------------------------------------------------------------------

class TestTableFiltering(unittest.TestCase):

    def test_excluded_tables_not_in_output(self):
        table_map = {"incident": "", "sys_audit": "", "syslog": ""}
        client = _make_client()
        client.make_request.return_value = {"result": _dict_fields("incident", "sys_id", "sys_updated_on")}

        p1, p2 = _patch_tables(table_map, sync_list=["incident"])
        with p1, p2:
            schemas, _ = get_dynamic_schema(client)

        self.assertIn("incident", schemas)
        self.assertNotIn("sys_audit", schemas)
        self.assertNotIn("syslog", schemas)

    def test_include_tables_config_respected(self):
        """Only tables in include_tables should appear in output."""
        table_map = {"incident": "", "problem": "", "change_request": ""}
        client = _make_client()
        client.make_request.return_value = {"result": _dict_fields("incident", "sys_id", "sys_updated_on")}

        p1, p2 = _patch_tables(table_map, sync_list=["incident"])
        with p1, p2:
            schemas, _ = get_dynamic_schema(client)

        self.assertIn("incident", schemas)
        self.assertNotIn("problem", schemas)
        self.assertNotIn("change_request", schemas)

    def test_empty_table_map_returns_empty_schemas(self):
        client = _make_client()
        p1, p2 = _patch_tables({}, sync_list=[])
        with p1, p2:
            schemas, mdata = get_dynamic_schema(client)
        self.assertEqual(schemas, {})
        self.assertEqual(mdata, {})


# ---------------------------------------------------------------------------
# Batch sys_dictionary queries
# ---------------------------------------------------------------------------

class TestBatchSysDictionary(unittest.TestCase):

    def test_uses_namein_operator(self):
        """
        sys_dictionary requests must use the nameIN<list> encoded-query
        operator, not repeated name= calls.
        """
        table_map = {"incident": "", "problem": ""}
        client = _make_client()

        # Return all fields for both tables in one batch response
        all_fields = (
            _dict_fields("incident", "sys_id", "sys_updated_on", "short_description")
            + _dict_fields("problem", "sys_id", "sys_updated_on")
        )
        client.make_request.return_value = {"result": all_fields}

        p1, p2 = _patch_tables(table_map)
        with p1, p2:
            get_dynamic_schema(client)

        for c in client.make_request.call_args_list:
            query = c[1].get("params", {}).get("sysparm_query", "")
            if "sys_dictionary" in c[1].get("endpoint", ""):
                self.assertIn("nameIN", query,
                              "sys_dictionary query must use nameIN operator, not name=")

    def test_fewer_api_calls_than_tables(self):
        """
        For N tables we should make ceil(N / CHUNK_SIZE) dict calls, not N.
        With CHUNK_SIZE=50 and 10 tables → exactly 1 dict call.
        """
        tables = {f"table_{i}": "" for i in range(10)}
        client = _make_client()
        # Return minimal fields covering all 10 tables
        all_fields = []
        for t in tables:
            all_fields += _dict_fields(t, "sys_id", "sys_updated_on")
        client.make_request.return_value = {"result": all_fields}

        p1, p2 = _patch_tables(tables)
        with p1, p2:
            get_dynamic_schema(client)

        dict_calls = [
            c for c in client.make_request.call_args_list
            if "sys_dictionary" in c[1].get("endpoint", "")
        ]
        # 10 tables, chunk=50 → 1 batch call
        self.assertEqual(len(dict_calls), 1)

    def test_chunk_boundary_creates_correct_number_of_requests(self):
        """
        52 tables with CHUNK_SIZE=50 → 2 batch calls for sys_dictionary.
        """
        tables = {f"t_{i}": "" for i in range(52)}
        client = _make_client()
        all_fields = []
        for t in tables:
            all_fields += _dict_fields(t, "sys_id", "sys_updated_on")
        # Both batch calls return the same pool (union is idempotent for our test)
        client.make_request.side_effect = [
            {"result": all_fields},  # batch 1
            {"result": all_fields},  # batch 2
        ]

        p1, p2 = _patch_tables(tables)
        with p1, p2:
            get_dynamic_schema(client)

        dict_calls = [
            c for c in client.make_request.call_args_list
            if "sys_dictionary" in c[1].get("endpoint", "")
        ]
        self.assertEqual(len(dict_calls), 2)

    def test_no_count_and_no_ref_link_in_dict_requests(self):
        table_map = {"incident": ""}
        client = _make_client()
        client.make_request.return_value = {
            "result": _dict_fields("incident", "sys_id", "sys_updated_on")
        }
        p1, p2 = _patch_tables(table_map)
        with p1, p2:
            get_dynamic_schema(client)

        for c in client.make_request.call_args_list:
            if "sys_dictionary" in c[1].get("endpoint", ""):
                p = c[1].get("params", {})
                self.assertEqual(p.get("sysparm_no_count"), "true")
                self.assertEqual(p.get("sysparm_exclude_reference_link"), "true")


# ---------------------------------------------------------------------------
# Table inheritance resolution
# ---------------------------------------------------------------------------

class TestInheritanceResolution(unittest.TestCase):

    def _run(self, table_map, sync_list, dict_fields_by_table):
        """
        Run get_dynamic_schema with controlled table_map, sync_list, and
        a single pooled sys_dictionary response.
        """
        client = _make_client()
        all_fields = []
        for t, fields in dict_fields_by_table.items():
            all_fields += _dict_fields(t, *fields)
        client.make_request.return_value = {"result": all_fields}

        p1, p2 = _patch_tables(table_map, sync_list=sync_list)
        with p1, p2:
            schemas, metadata = get_dynamic_schema(client)
        return schemas, metadata

    def test_child_inherits_parent_fields(self):
        """
        incident extends task → incident schema must include task's fields.
        """
        table_map = {"task": "", "incident": "task"}
        # task owns 'priority'; incident owns 'caller_id'
        fields_by_table = {
            "task":     ["sys_id", "sys_updated_on", "priority"],
            "incident": ["sys_id", "sys_updated_on", "caller_id"],
        }
        schemas, _ = self._run(table_map, ["incident", "task"], fields_by_table)

        incident_props = schemas.get("incident", {}).get("properties", {})
        self.assertIn("priority", incident_props,
                      "Inherited field 'priority' from task must appear in incident schema")
        self.assertIn("caller_id", incident_props,
                      "Own field 'caller_id' must appear in incident schema")

    def test_child_field_overrides_parent(self):
        """
        When parent and child define the same field, the child's definition wins.
        """
        from tap_servicenow.streams import servicenow_type_to_json_type

        table_map = {"task": "", "incident": "task"}
        # Both define 'description'; task as plain string, incident as html
        task_fields = [
            {"name": "task",     "element": "sys_id",      "internal_type": "string"},
            {"name": "task",     "element": "description",  "internal_type": "string"},
            {"name": "task",     "element": "sys_updated_on", "internal_type": "glide_date_time"},
        ]
        incident_fields = [
            {"name": "incident", "element": "sys_id",      "internal_type": "string"},
            {"name": "incident", "element": "description",  "internal_type": "html"},
            {"name": "incident", "element": "sys_updated_on", "internal_type": "glide_date_time"},
        ]
        all_fields = task_fields + incident_fields
        client = _make_client()
        client.make_request.return_value = {"result": all_fields}

        p1, p2 = _patch_tables(table_map, sync_list=["incident", "task"])
        with p1, p2:
            schemas, _ = get_dynamic_schema(client)

        incident_desc = schemas.get("incident", {}).get("properties", {}).get("description", {})
        expected = servicenow_type_to_json_type("html")
        self.assertEqual(incident_desc, expected,
                         "Child field 'description' (html) must override parent's (string)")

    def test_multi_level_inheritance(self):
        """
        grandchild → child → parent: grandchild must see all three levels.
        """
        table_map = {"base": "", "mid": "base", "leaf": "mid"}
        fields_by_table = {
            "base": ["sys_id", "sys_updated_on", "base_field"],
            "mid":  ["sys_id", "sys_updated_on", "mid_field"],
            "leaf": ["sys_id", "sys_updated_on", "leaf_field"],
        }
        schemas, _ = self._run(table_map, ["leaf"], fields_by_table)

        leaf_props = schemas.get("leaf", {}).get("properties", {})
        self.assertIn("base_field", leaf_props)
        self.assertIn("mid_field",  leaf_props)
        self.assertIn("leaf_field", leaf_props)

    def test_table_with_no_parent_unaffected(self):
        """Tables without a super_class must not be altered by the walk."""
        table_map = {"sys_user": ""}
        fields_by_table = {"sys_user": ["sys_id", "sys_updated_on", "email"]}
        schemas, _ = self._run(table_map, ["sys_user"], fields_by_table)

        props = schemas.get("sys_user", {}).get("properties", {})
        self.assertIn("sys_id", props)
        self.assertIn("email", props)

    def test_inheritance_cycle_does_not_hang(self):
        """A cycle in super_class references must not cause infinite recursion."""
        # a→b→a is a cycle
        table_map = {"a": "b", "b": "a"}
        fields_by_table = {
            "a": ["sys_id", "sys_updated_on", "field_a"],
            "b": ["sys_id", "sys_updated_on", "field_b"],
        }
        # Should complete without RecursionError / hanging
        try:
            schemas, _ = self._run(table_map, ["a", "b"], fields_by_table)
        except RecursionError:
            self.fail("Cycle in super_class chain caused RecursionError")

    def test_sys_id_always_in_schema(self):
        """sys_id must always be present even if not in sys_dictionary."""
        table_map = {"no_sys_id_table": ""}
        fields_by_table = {"no_sys_id_table": ["some_field"]}
        schemas, _ = self._run(table_map, ["no_sys_id_table"], fields_by_table)
        props = schemas.get("no_sys_id_table", {}).get("properties", {})
        self.assertIn("sys_id", props)

    def test_sys_updated_on_absent_means_full_table(self):
        """A table with no sys_updated_on in sys_dictionary must be treated as
        FULL_TABLE.  The field must NOT be injected into the schema."""
        from singer import metadata
        table_map = {"no_dt_table": ""}
        fields_by_table = {"no_dt_table": ["some_field"]}
        schemas, field_metadata = self._run(table_map, ["no_dt_table"], fields_by_table)
        props = schemas.get("no_dt_table", {}).get("properties", {})
        self.assertNotIn("sys_updated_on", props)
        # Metadata must declare FULL_TABLE replication
        mdata = metadata.to_map(field_metadata.get("no_dt_table", []))
        self.assertEqual(
            metadata.get(mdata, (), "forced-replication-method") or
            metadata.get(mdata, (), "replication-method"),
            "FULL_TABLE",
        )


# ---------------------------------------------------------------------------
# Unauthorised table handling (deferred summary logging)
# ---------------------------------------------------------------------------

class TestUnauthorisedTableHandling(unittest.TestCase):

    def test_403_table_skipped_and_logged(self):
        """
        A table returning 403 on access check must be skipped (not in schemas)
        and a WARNING must be logged.  singer.get_logger() binds to the root
        logger, so assertLogs must not specify a logger name.
        """
        table_map = {"incident": "", "forbidden_table": ""}
        client = _make_client()
        client.make_request.return_value = {
            "result": (
                _dict_fields("incident", "sys_id", "sys_updated_on")
                + _dict_fields("forbidden_table", "sys_id", "sys_updated_on")
            )
        }

        def fake_get(table, params=None):
            if table == "forbidden_table":
                raise ServiceNowForbiddenError(
                    "HTTP-error-code: 403, Error: You are missing the following required scopes: read"
                )

        client.get.side_effect = fake_get

        p1, p2 = _patch_tables(table_map, sync_list=["incident", "forbidden_table"])
        with p1, p2:
            # No logger-name arg: catches root logger used by singer.get_logger()
            with self.assertLogs(level="WARNING") as log:
                schemas, _ = get_dynamic_schema(client)

        self.assertNotIn("forbidden_table", schemas)
        self.assertIn("incident", schemas)
        # Warning message must reference the forbidden table
        combined = " ".join(log.output)
        self.assertIn("forbidden_table", combined)

    def test_all_tables_forbidden_raises_exception(self):
        """If every table is 403, discovery must raise an Exception."""
        table_map = {"table_a": "", "table_b": ""}
        client = _make_client()
        client.make_request.return_value = {
            "result": (
                _dict_fields("table_a", "sys_id", "sys_updated_on")
                + _dict_fields("table_b", "sys_id", "sys_updated_on")
            )
        }
        client.get.side_effect = ServiceNowForbiddenError(
            "HTTP-error-code: 403, Error: You are missing the following required scopes: read"
        )

        p1, p2 = _patch_tables(table_map, sync_list=["table_a", "table_b"])
        with p1, p2:
            with self.assertRaises(Exception) as ctx:
                get_dynamic_schema(client)

        self.assertIn("403", str(ctx.exception))

    def test_table_with_no_fields_skipped(self):
        """A table for which sys_dictionary returns no fields must be skipped."""
        table_map = {"empty_table": ""}
        client = _make_client()
        client.make_request.return_value = {"result": []}  # no fields

        p1, p2 = _patch_tables(table_map, sync_list=["empty_table"])
        with p1, p2:
            schemas, _ = get_dynamic_schema(client)

        self.assertNotIn("empty_table", schemas)

    def test_access_check_includes_no_count(self):
        """Access-check GET must include sysparm_no_count=true."""
        table_map = {"incident": ""}
        client = _make_client()
        client.make_request.return_value = {
            "result": _dict_fields("incident", "sys_id", "sys_updated_on")
        }

        p1, p2 = _patch_tables(table_map, sync_list=["incident"])
        with p1, p2:
            get_dynamic_schema(client)

        # client.get(table=..., params=...) is the access-check call
        _, kwargs = client.get.call_args
        params = kwargs.get("params", {})
        self.assertEqual(params.get("sysparm_no_count"), "true")


# ---------------------------------------------------------------------------
# Singer metadata consistency
# ---------------------------------------------------------------------------

class TestMetadataOutput(unittest.TestCase):

    def _schemas_and_meta(self, table_name="incident"):
        table_map = {table_name: ""}
        client = _make_client()
        client.make_request.return_value = {
            "result": _dict_fields(table_name, "sys_id", "sys_updated_on", "name")
        }
        p1, p2 = _patch_tables(table_map, sync_list=[table_name])
        with p1, p2:
            return get_dynamic_schema(client)

    def test_metadata_entry_exists_for_each_schema(self):
        schemas, meta = self._schemas_and_meta()
        self.assertEqual(set(schemas.keys()), set(meta.keys()))

    def test_replication_key_marked_automatic(self):
        from singer import metadata as sm
        _, meta = self._schemas_and_meta()
        mmap = sm.to_map(meta["incident"])
        inclusion = sm.get(mmap, ("properties", "sys_updated_on"), "inclusion")
        self.assertEqual(inclusion, "automatic")

    def test_key_properties_is_sys_id(self):
        from singer import metadata as sm
        _, meta = self._schemas_and_meta()
        mmap = sm.to_map(meta["incident"])
        key_props = sm.get(mmap, (), "table-key-properties")
        self.assertEqual(key_props, ["sys_id"])


if __name__ == "__main__":
    unittest.main()
