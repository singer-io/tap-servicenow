import copy
import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class ServiceNowBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2019-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-servicenow"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.servicenow"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams.

        Streams listed here are those confirmed to have records in the
        venqlikdi sandbox (see data11.json).  All are INCREMENTAL on
        sys_updated_on with page_size=1000.
        """
        default = {
            cls.PRIMARY_KEYS: {"sys_id"},
            cls.REPLICATION_METHOD: cls.INCREMENTAL,
            cls.REPLICATION_KEYS: {"sys_updated_on"},
            cls.RESPECTS_START_DATE: False,
            cls.API_LIMIT: 1000,
        }
        return {
            "fm_expense_line": default,
            "sysapproval_group": default,
            "cmdb_class_info": default,
            "metric_instance": default,
            "promin_scheduled_task": default,
            "sn_cmdb_ws_ms_ci_dashboard_data": default,
            "sn_cmdb_ws_base_aggregate_data": default,
            "sys_report_map_source": default,
            "cmdb_dynamic_ire_feature": default,
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "instance": "TAP_SERVICENOW_INSTANCE",
            "user": "TAP_SERVICENOW_USER",
            "password": "TAP_SERVICENOW_PASSWORD",
        }

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": "2022-07-01T00:00:00Z",
            "include_tables": list(self.expected_stream_names()),
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def parse_date(date_value):
        """Extend base parse_date to handle all datetime formats used by this tap.
        """
        # --- ServiceNow native state format ---------------------------------
        snow_format = "%Y-%m-%d %H:%M:%S"
        try:
            parsed = dt.strptime(date_value, snow_format)
            return parsed.replace(tzinfo=pytz.UTC)
        except (ValueError, TypeError):
            pass

        for iso_z_fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                parsed = dt.strptime(date_value, iso_z_fmt)
                return parsed.replace(tzinfo=pytz.UTC)
            except (ValueError, TypeError):
                pass

        # Fall back to the tap-tester base implementation for other formats.
        return BaseCase.parse_date(date_value)


    def run_and_verify_check_mode(self, conn_id):
        """Override to support dynamic discovery.

        The tap discovers all ServiceNow tables dynamically (hundreds of
        streams). Rather than requiring expected_stream_names() to list
        every discovered table, this override verifies that the expected
        streams are a *subset* of what was discovered and returns only the
        filtered catalogs for the expected streams.
        """
        check_job_name = runner.run_check_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        all_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(all_catalogs), 0,
                           logging="A catalog was produced by discovery.")

        found_stream_names = {catalog['stream_name'] for catalog in all_catalogs}
        expected_stream_names = self.expected_stream_names()

        missing_streams = expected_stream_names - found_stream_names
        self.assertEqual(
            set(),
            missing_streams,
            logging="All expected streams were found in the discovered catalog.",
        )

        # Return only the catalogs for the streams we care about
        return [c for c in all_catalogs if c['stream_name'] in expected_stream_names]
