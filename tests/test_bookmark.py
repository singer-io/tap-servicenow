from base import ServiceNowBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class ServiceNowBookMarkTest(BookmarkTest, ServiceNowBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%d %H:%M:%S"
    initial_bookmarks = {
        "bookmarks": {
            "metric_instance": { "sys_updated_on" : "2026-03-01T00:00:00Z"},
            "cmdb_class_info": { "sys_updated_on" : "2025-01-09T00:00:00Z"},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_servicenow_bookmark_test"

    def streams_to_test(self):
        # Exclude tiny streams (<3 records) or streams have date for same date to keep bookmark tests meaningful.
        streams_to_exclude = {"sysapproval_group", "fm_expense_line", "sysevent_email_action",
                              "promin_scheduled_task", "sn_cmdb_ws_ms_ci_dashboard_data",
                              "sn_cmdb_ws_base_aggregate_data", "sys_report_map_source",
                              "cmdb_dynamic_ire_feature"}
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""
        new_bookmarks = {
            "metric_instance": { "sys_updated_on" : "2026-03-09T00:00:00Z"},
            "cmdb_class_info": { "sys_updated_on" : "2025-01-15T00:00:00Z"},

        }

        return new_bookmarks
