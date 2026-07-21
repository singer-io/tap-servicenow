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
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

from tap_servicenow.schema import get_dynamic_schema
from tap_servicenow.streams import DEFAULT_EXCLUDED_TABLES
from tap_servicenow.concurrent_discovery import (
    ConcurrentDiscovery,
    ServiceNowDictionaryFetcher,
    ServiceNowTableSchemaBuilder,
)
from tap_servicenow.exceptions import (
    ServiceNowForbiddenError,
    ServiceNowServiceUnavailableError,
)


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

    def test_incremental_access_check_uses_replication_key_probe(self):
        """Incremental tables must probe with sys_updated_on somewhere in the call chain."""
        table_map = {"incident": ""}
        client = _make_client(config={"start_date": "2026-01-02T03:04:05Z"})
        client.make_request.return_value = {
            "result": _dict_fields("incident", "sys_id", "sys_updated_on")
        }

        p1, p2 = _patch_tables(table_map, sync_list=["incident"])
        with p1, p2:
            get_dynamic_schema(client)

        # At least one client.get() call must include a sys_updated_on filter;
        # which call carries it is an implementation detail of the field-check path.
        all_queries = [
            kw.get("params", {}).get("sysparm_query", "")
            for _, kw in client.get.call_args_list
        ]
        self.assertTrue(
            any("sys_updated_on>=" in q for q in all_queries),
            f"No call to client.get() used a sys_updated_on filter; queries seen: {all_queries}",
        )

    def test_full_table_access_check_skips_replication_key_probe(self):
        """FULL_TABLE streams must never use a sys_updated_on filter in any probe call."""
        table_map = {"no_dt_table": ""}
        client = _make_client(config={"start_date": "2026-01-02T03:04:05Z"})
        client.make_request.return_value = {
            "result": _dict_fields("no_dt_table", "sys_id", "name")
        }

        p1, p2 = _patch_tables(table_map, sync_list=["no_dt_table"])
        with p1, p2:
            get_dynamic_schema(client)

        # None of the client.get() calls should reference sys_updated_on.
        all_queries = [
            kw.get("params", {}).get("sysparm_query", "")
            for _, kw in client.get.call_args_list
        ]
        self.assertFalse(
            any("sys_updated_on" in q for q in all_queries),
            f"A FULL_TABLE probe used sys_updated_on; queries seen: {all_queries}",
        )


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


# ---------------------------------------------------------------------------
# sys_dictionary fetch pagination (ServiceNowDictionaryFetcher.process_item)
# ---------------------------------------------------------------------------

class TestDictionaryFetcherPagination(unittest.TestCase):
    """A chunk with more dictionary rows than the page limit must not lose fields."""

    def _client(self, pages):
        c = MagicMock()
        c.base_url = "https://test.service-now.com/api/now/table"
        c.make_request.side_effect = [{"result": p} for p in pages]
        return c

    def test_paginates_past_the_page_limit(self):
        # dict_page_size=2: page1 is full, page2 is short, page3 is empty -> stop
        page1 = [{"name": "t1", "element": "f1", "internal_type": "string", "sys_id": "s1"},
                 {"name": "t1", "element": "f2", "internal_type": "string", "sys_id": "s2"}]
        page2 = [{"name": "t1", "element": "f3", "internal_type": "string", "sys_id": "s3"}]
        client = self._client([page1, page2, []])
        fetcher = ServiceNowDictionaryFetcher(client, dict_page_size=2)
        result = fetcher.process_item(["t1"])
        self.assertEqual(set(result["t1"].keys()), {"f1", "f2", "f3"})
        self.assertEqual(client.make_request.call_count, 3)

    def test_short_page_does_not_stop_pagination(self):
        """Row-level ACLs are applied post-query, so a short page is not the last
        page (KB0727636). Stopping there silently drops dictionary fields."""
        page1 = [{"name": "t1", "element": "f1", "internal_type": "string", "sys_id": "s1"}]
        page2 = [{"name": "t1", "element": "f2", "internal_type": "string", "sys_id": "s2"}]
        client = self._client([page1, page2, []])
        fetcher = ServiceNowDictionaryFetcher(client, dict_page_size=50)
        result = fetcher.process_item(["t1"])
        self.assertEqual(set(result["t1"].keys()), {"f1", "f2"})
        self.assertEqual(client.make_request.call_count, 3)

    def test_stops_on_empty_page(self):
        client = self._client([[]])
        fetcher = ServiceNowDictionaryFetcher(client, dict_page_size=50)
        self.assertEqual(fetcher.process_item(["t1"]), {})
        self.assertEqual(client.make_request.call_count, 1)

    def test_stall_guard_stops_when_cursor_cannot_advance(self):
        """A page carrying no advanceable sys_id must not loop forever."""
        stalled = [{"name": "t1", "element": "f1", "internal_type": "string", "sys_id": ""}]
        client = self._client([stalled, stalled, stalled])
        fetcher = ServiceNowDictionaryFetcher(client, dict_page_size=50)
        result = fetcher.process_item(["t1"])
        self.assertEqual(set(result["t1"].keys()), {"f1"})
        self.assertEqual(client.make_request.call_count, 1)

    def test_request_failure_raises_instead_of_returning_partial(self):
        """make_request already retries transients (RETRY_ON_TRANSIENT), so a failure
        here is persistent. Emitting the partial page would put a table in the catalog
        with silently missing columns."""
        page1 = [{"name": "t1", "element": "f1", "internal_type": "string", "sys_id": "s1"}]
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.make_request.side_effect = [{"result": page1}, RuntimeError("boom")]
        fetcher = ServiceNowDictionaryFetcher(client, dict_page_size=1)
        with self.assertRaises(RuntimeError):
            fetcher.process_item(["t1"])

    def test_fetch_propagates_dictionary_failure(self):
        """The dictionary fetch is catalog-wide, so a chunk failure must abort
        discovery rather than be swallowed by the thread pool."""
        client = MagicMock()
        client.base_url = "https://test.service-now.com/api/now/table"
        client.make_request.side_effect = RuntimeError("boom")
        fetcher = ServiceNowDictionaryFetcher(client, dict_page_size=50, max_workers=1)
        with self.assertRaises(RuntimeError):
            fetcher.fetch(["t1"], chunk_size=1)

    def test_fail_fast_is_opt_in_per_phase(self):
        """Phases that legitimately skip individual items (the per-table schema
        build drops tables the account cannot read) must keep swallowing."""
        self.assertTrue(ServiceNowDictionaryFetcher.FAIL_FAST)
        self.assertFalse(ServiceNowTableSchemaBuilder.FAIL_FAST)

        class _Skipping(ConcurrentDiscovery):
            def process_item(self, item):
                raise RuntimeError("boom")

        self.assertEqual(_Skipping(max_workers=1).run(["a", "b"]), [])


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Inheritance resolution: thread safety and cycles
# ---------------------------------------------------------------------------

class TestResolveFieldsConcurrency(unittest.TestCase):
    """_resolve_fields walks the super_class chain under a thread pool.

    The previous implementation recursed while holding a per-table lock, which
    deadlocked across threads on a cyclic chain (A->B->A): one thread holds
    lock[A] wanting lock[B] while another holds lock[B] wanting lock[A].
    ThreadPoolExecutor has no timeout, so discovery hung forever with no output.
    """

    @staticmethod
    def _builder(table_map, field_map):
        return ServiceNowTableSchemaBuilder(
            MagicMock(), field_map, table_map, max_workers=4
        )

    def test_cyclic_chain_does_not_deadlock(self):
        builder = self._builder({"A": "B", "B": "A"},
                                {"A": {"f_a": {}}, "B": {"f_b": {}}})
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(builder._resolve_fields, t)
                       for t in ["A", "B", "A", "B"]]
            # A deadlock shows up as TimeoutError here rather than a hang.
            results = [f.result(timeout=10) for f in futures]
        self.assertTrue(all(r for r in results))

    def test_cyclic_chain_is_deterministic(self):
        """A cycle used to yield different schemas depending on cache order."""
        builder = self._builder({"A": "B", "B": "A"},
                                {"A": {"f_a": {}}, "B": {"f_b": {}}})
        self.assertEqual(sorted(builder._resolve_fields("A")), ["f_a", "f_b"])
        self.assertEqual(sorted(builder._resolve_fields("B")), ["f_a", "f_b"])

    def test_child_overrides_parent(self):
        """Merge order must stay root-first so the child definition wins."""
        builder = self._builder(
            {"child": "parent", "parent": ""},
            {"parent": {"shared": "PARENT", "only_parent": {}},
             "child": {"shared": "CHILD"}},
        )
        resolved = builder._resolve_fields("child")
        self.assertEqual(resolved["shared"], "CHILD")
        self.assertIn("only_parent", resolved)

    def test_deep_chain_resolves_fully(self):
        builder = self._builder(
            {"d": "c", "c": "b", "b": "a", "a": ""},
            {"a": {"fa": {}}, "b": {"fb": {}}, "c": {"fc": {}}, "d": {"fd": {}}},
        )
        self.assertEqual(sorted(builder._resolve_fields("d")),
                         ["fa", "fb", "fc", "fd"])


class TestErroredVsUnauthorizedTables(unittest.TestCase):
    """A transient failure must not be reported as a permission problem.

    Dropping a table on any exception made an outage look identical to "the
    account cannot read this", and because it was never recorded as
    unauthorized, schema.py's all-tables-blocked check never fired either. The
    table just vanished from the catalog.
    """

    @staticmethod
    def _builder(client):
        return ServiceNowTableSchemaBuilder(
            client, {"incident": {"sys_id": {}, "sys_updated_on": {}}},
            {"incident": ""}, max_workers=1,
        )

    def test_permission_error_recorded_as_unauthorized(self):
        client = MagicMock()
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.get.side_effect = ServiceNowForbiddenError("403")
        builder = self._builder(client)

        self.assertIsNone(builder.process_item("incident"))
        self.assertEqual(builder.unauthorized_tables, ["incident"])
        self.assertEqual(builder.errored_tables, [])

    def test_transient_error_recorded_separately(self):
        client = MagicMock()
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.get.side_effect = ServiceNowServiceUnavailableError("503")
        builder = self._builder(client)

        self.assertIsNone(builder.process_item("incident"))
        self.assertEqual(builder.unauthorized_tables, [],
                         "a 503 is not a permission answer")
        self.assertEqual([t for t, _ in builder.errored_tables], ["incident"])
