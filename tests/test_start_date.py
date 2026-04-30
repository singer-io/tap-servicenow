from base import ServiceNowBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest



class ServiceNowStartDateTest(StartDateTest, ServiceNowBaseTest):
    """Verify tap start_date behavior for incremental streams.
    """

    @staticmethod
    def name():
        return "tap_tester_servicenow_start_date_test"

    def streams_to_test(self):
        # Exclude streams whose oldest record is newer than start_date_2 (2026-04-01).
        streams_to_exclude = {
            "sys_report_map_source",
            "sn_cmdb_ws_base_aggregate_data",
            "sn_cmdb_ws_ms_ci_dashboard_data",
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2022-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2026-04-01T00:00:00Z"
