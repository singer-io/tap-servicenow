
from base import ServiceNowBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest
from tap_tester.logger import LOGGER


class ServiceNowInterruptedSyncTest(InterruptedSyncTest, ServiceNowBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_servicenow_interrupted_sync_test"

    def streams_to_test(self):
        streams_to_exclude = {"fm_expense_line"}
        return self.expected_stream_names().difference(streams_to_exclude)

    def manipulate_state(self):
        """Return a partial state that simulates an interrupted sync.
        """
        return {
            "currently_syncing": "promin_scheduled_task",
            "bookmarks": {
                "promin_scheduled_task": {
                    "sys_updated_on": "2022-07-01T00:00:00Z",
                },
                "sys_report_map_source": {
                    "sys_updated_on": "2022-07-01T00:00:00Z",
                }
            }
        }

    def test_resuming_sync_records(self):
        """Override the base test_resuming_sync_records.
        """
        incremental_streams = {s for s, m in self.expected_replication_method().items()
                               if m == self.INCREMENTAL}
        currently_syncing = self.manipulate_state()['currently_syncing']
        replication_key = next(iter(self.expected_replication_keys(currently_syncing)))

        # 1. Every stream must have at least one record in the resuming sync.
        for stream in self.streams_to_test():
            with self.subTest(stream=stream, msg="stream has at least 1 resuming record"):
                record_count = self.record_count_by_stream.get(stream, 0)
                self.assertGreater(record_count, 0,
                                   logging=f"verify {stream} synced records in resuming sync")

        # 2. Interrupted stream: every resuming record >= manipulated bookmark.
        interrupted_bookmark = self.manipulate_state()['bookmarks'][currently_syncing][
            replication_key]
        resuming_records_for_interrupted = [
            record['data'] for record in
            self.resuming_sync_records.get(currently_syncing, {}).get('messages', [])
            if record.get('action') == 'upsert']

        for record in resuming_records_for_interrupted:
            record_date = record.get(replication_key)
            if record_date is None:
                continue
            with self.subTest(stream=currently_syncing,
                              msg="interrupted stream record respects bookmark"):
                self.assertGreaterEqual(
                    self.parse_date(record_date),
                    self.parse_date(interrupted_bookmark),
                    logging=f"verify {currently_syncing} record {record_date} >= "
                            f"manipulated bookmark {interrupted_bookmark}")

        # 3. Final state bookmark >= manipulated bookmark for bookmarked streams.
        for stream in self.streams_to_test().intersection(incremental_streams):
            with self.subTest(stream=stream,
                              msg="resuming state bookmark moved forward"):
                manipulated = self.manipulate_state()['bookmarks'].get(stream, {}).get(
                    next(iter(self.expected_replication_keys(stream))))
                if manipulated is None:
                    continue
                resuming_bm = self.resuming_sync_state.get('bookmarks', {}).get(stream, {}).get(
                    next(iter(self.expected_replication_keys(stream))))
                if resuming_bm is None:
                    continue
                self.assertGreaterEqual(
                    self.parse_date(resuming_bm),
                    self.parse_date(manipulated),
                    logging=f"verify {stream} final bookmark {resuming_bm} >= "
                            f"manipulated bookmark {manipulated}")
