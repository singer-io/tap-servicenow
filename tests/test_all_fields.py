from base import ServiceNowBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

KNOWN_MISSING_FIELDS = {

}

KEYS_WITH_NO_DATA = {
    "cmdb_class_info": {
        "sys_scope",
    },
    "cmdb_dynamic_ire_feature": {
        "sys_scope",
    },
    "fm_expense_line": {
        "fixed_asset",
    },
    "promin_scheduled_task": {
        "ml_solution_id",
    },
    "sys_attachment_doc": {
        "sys_updated_on",
    },
    "sysapproval_group": {
        "wf_activity",
        "delivery_plan",
        "delivery_task",
    },
    "sysevent_email_action": {
        "message_html",
        "subject",
        "message_text",
        "message",
        "digest_template",
        "sms_alternate",
        "template",
    },
}


class ServiceNowAllFields(AllFieldsTest, ServiceNowBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    KEYS_WITH_NO_DATA = KEYS_WITH_NO_DATA

    @staticmethod
    def name():
        return "tap_tester_servicenow_all_fields_test"

    def streams_to_test(self):
        "Removing it too much data for same day taking too much time to run the test"
        streams_to_exclude = {"fm_expense_line"}
        return self.expected_stream_names().difference(streams_to_exclude)

