from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import ServiceNowBaseTest

class ServiceNowPaginationTest(PaginationTest, ServiceNowBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """
    start_date = "2026-01-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_servicenow_pagination_test"

    def get_properties(self, original: bool = True):
        props = super().get_properties(original=original)
        props["start_date"] = self.start_date
        return props

    def streams_to_test(self):
        # Limit to streams that span more than one page (>1000 records).
        return {"fm_expense_line"}
