import unittest
from contextlib import suppress

import requests
from unittest.mock import patch
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from tap_servicenow.datetime_utils import InvalidDatetimeError, to_snow_dt
from tap_servicenow.client import (
    Client,
    MAX_BACKOFF_SECONDS,
    RETRY_ON_TRANSIENT,
    retry_after_or_expo,
)
from tap_servicenow.exceptions import (
    ServiceNowBadGatewayError,
    ServiceNowBadRequestError,
    ServiceNowConflictError,
    ServiceNowForbiddenError,
    ServiceNowInternalServerError,
    ServiceNowNotFoundError,
    ServiceNowNotImplementedError,
    ServiceNowRateLimitError,
    ServiceNowServiceUnavailableError,
    ServiceNowUnauthorizedError,
    ServiceNowUnprocessableEntityError,
)


default_config = {
    "instance": "mock-instance",
    "base_url": "https://api.example.com",
    "request_timeout": 30,
    "auth_token": "dummy_token",
    "user": "mock-user",
    "password": "mock-pass",
}

DEFAULT_REQUEST_TIMEOUT = 300

class MockResponse:
    """Mocked standard HTTPResponse to test error handling."""

    def __init__(
        self, status_code, resp = "", content=[""], headers=None, raise_error=True, text={}
    ):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def __bool__(self):
        """Mirror requests.Response.__bool__, which returns self.ok.

        Without this the double is truthy for 4xx/5xx while the real object is
        falsy, so a `if response:` guard passes here and fails in production.
        That is exactly how the Retry-After parsing bug survived a green suite.
        """
        return self.status_code < 400

    def raise_for_status(self):
        """If an error occur, this method returns a HTTPError object.

        Raises:
            requests.HTTPError: Mock http error.

        Returns:
            int: Returns status code if not error occurred.
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        """Returns a JSON object of the result."""
        return self.text

class TestClient(unittest.TestCase):

    def setUp(self):
        """Set up the client with default configuration."""
        self.client = Client(default_config)

    @parameterized.expand([    
        ["empty value", "", DEFAULT_REQUEST_TIMEOUT],
        ["string value", "12", 12.0],
        ["integer value", 10, 10.0],
        ["float value", 20.0, 20.0],
        ["zero value", 0, DEFAULT_REQUEST_TIMEOUT]
    ])
    @patch("tap_servicenow.client.session")
    def test_client_initialization(self, test_name, input_value, expected_value, mock_session):
        default_config["request_timeout"] = input_value
        client = Client(default_config)
        assert client.request_timeout == expected_value
        assert isinstance(client._session, mock_session().__class__)

    @parameterized.expand([
        ["400 error", 400, MockResponse(400), ServiceNowBadRequestError, "A validation exception has occurred."],
        ["401 error", 401, MockResponse(401), ServiceNowUnauthorizedError, "The access token provided is expired, revoked, malformed or invalid for other reasons."],
        ["403 error", 403, MockResponse(403), ServiceNowForbiddenError, "You are missing the following required scopes: read"],
        ["404 error", 404, MockResponse(404), ServiceNowNotFoundError, "The resource you have specified cannot be found."],
        ["409 error", 409, MockResponse(409), ServiceNowConflictError, "The API request cannot be completed because the requested operation would conflict with an existing item."],
        ["422 error", 422, MockResponse(422), ServiceNowUnprocessableEntityError, "The request content itself is not processable by the server."],
    ])
    def test_make_request_http_failure_without_retry(self, test_name, error_code, mock_response, error, error_message):
        
        with patch.object(self.client._session, "request", return_value=mock_response):
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

        expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
        self.assertEqual(str(e.exception), expected_error_message)

    @parameterized.expand([
        ["429 error", 429, MockResponse(429, headers={"Retry-After": "2"}), ServiceNowRateLimitError, "The API rate limit for your organisation/application pairing has been exceeded. (Retry after 2 seconds.)"],
        ["500 error", 500, MockResponse(500), ServiceNowInternalServerError, "The server encountered an unexpected condition which prevented it from fulfilling the request."],
        ["501 error", 501, MockResponse(501), ServiceNowNotImplementedError, "The server does not support the functionality required to fulfill the request."],
        ["502 error", 502, MockResponse(502), ServiceNowBadGatewayError, "Server received an invalid response."],
        ["503 error", 503, MockResponse(503), ServiceNowServiceUnavailableError, "API service is currently unavailable."],
    ])
    @patch("time.sleep")
    def test_make_request_http_failure_with_retry(self, test_name, error_code, mock_response, error, error_message, mock_sleep):
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
            self.assertEqual(str(e.exception), expected_error_message)
            self.assertEqual(mock_request.call_count, 5)

    @parameterized.expand([
        ["ConnectionResetError", ConnectionResetError],
        ["ConnectionError", ConnectionError],
        ["ChunkedEncodingError", ChunkedEncodingError],
        ["Timeout", Timeout],
    ])
    @patch("time.sleep")
    def test_make_request_other_failure_with_retry(self, test_name, error, mock_sleep):

        with patch.object(self.client._session, "request", side_effect=error) as mock_request:
            with self.assertRaises(error):
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            self.assertEqual(mock_request.call_count, 5)

    # --- Access-probe (client.get) retry policy -------------------------------
    # get() is the per-table discovery probe. Without retry, a single transient
    # 429 during discovery would silently drop a table the account can read.

    @patch("time.sleep")
    def test_get_probe_retries_then_succeeds_on_429(self, mock_sleep):
        """A transient 429 on the probe is retried, not treated as no-access."""
        responses = [MockResponse(429, headers={"Retry-After": "1"}), MockResponse(200)]
        with patch.object(self.client._session, "get", side_effect=responses) as mock_get:
            self.client.get("incident")  # must not raise
            self.assertEqual(mock_get.call_count, 2)

    @patch("time.sleep")
    def test_get_probe_retries_5x_on_persistent_429(self, mock_sleep):
        with patch.object(self.client._session, "get",
                          return_value=MockResponse(429, headers={"Retry-After": "1"})) as mock_get:
            with self.assertRaises(ServiceNowRateLimitError):
                self.client.get("incident")
            self.assertEqual(mock_get.call_count, 5)

    def test_get_probe_403_not_retried(self):
        """403 is a real permission answer - raise immediately, do not retry."""
        with patch.object(self.client._session, "get",
                          return_value=MockResponse(403)) as mock_get:
            with self.assertRaises(ServiceNowForbiddenError):
                self.client.get("incident")
            self.assertEqual(mock_get.call_count, 1)


class TestGetTotalCount(unittest.TestCase):
    """Tests for Client.get_total_count() — reads X-Total-Count from headers."""

    def setUp(self):
        self.client = Client(default_config)

    def _resp(self, status, count_header=None):
        headers = {}
        if count_header is not None:
            headers["X-Total-Count"] = str(count_header)
        return MockResponse(status, headers=headers, raise_error=(status >= 400), text={})

    def test_returns_count_from_header(self):
        """Returns the integer value of X-Total-Count when present."""
        with patch.object(self.client._session, "get",
                          return_value=self._resp(200, count_header=42)):
            result = self.client.get_total_count("https://test.example.com/incident")
        self.assertEqual(result, 42)

    def test_returns_none_when_header_absent(self):
        """Returns None when X-Total-Count is not in the response headers."""
        with patch.object(self.client._session, "get",
                          return_value=self._resp(200)):
            result = self.client.get_total_count("https://test.example.com/incident")
        self.assertIsNone(result)

    def test_raises_on_403(self):
        """A 403 response must raise ServiceNowForbiddenError immediately."""
        with patch.object(self.client._session, "get",
                          return_value=self._resp(403)) as mock_get:
            with self.assertRaises(ServiceNowForbiddenError):
                self.client.get_total_count("https://test.example.com/incident")
            self.assertEqual(mock_get.call_count, 1)

    @patch("time.sleep")
    def test_retries_on_429_then_succeeds(self, mock_sleep):
        """A transient 429 is retried and does not propagate if it clears."""
        responses = [
            self._resp(429),
            self._resp(200, count_header=10),
        ]
        with patch.object(self.client._session, "get", side_effect=responses) as mock_get:
            result = self.client.get_total_count("https://test.example.com/incident")
        self.assertEqual(result, 10)
        self.assertEqual(mock_get.call_count, 2)

    def test_probe_uses_limit_1_and_no_count_absent(self):
        """Probe must request sysparm_limit=1 and NOT include sysparm_no_count.

        sysparm_no_count suppresses X-Total-Count; it must be absent from the
        probe request even when it appears in the stream's base params.
        """
        captured_params = {}

        def capture(url, **kwargs):
            captured_params.update(kwargs.get("params", {}))
            return self._resp(200, count_header=5)

        with patch.object(self.client._session, "get", side_effect=capture):
            self.client.get_total_count(
                "https://test.example.com/incident",
                params={"sysparm_no_count": "true", "sysparm_query": "active=true"},
            )

        self.assertEqual(captured_params.get("sysparm_limit"), 1)
        self.assertEqual(captured_params.get("sysparm_offset"), 0)
        self.assertNotIn("sysparm_no_count", captured_params)


class TestRetryWaitSchedule(unittest.TestCase):
    """The wait between retries: Retry-After exactly, expo otherwise.

    Both properties were wrong when the Retry-After wait lived in an
    on_backoff handler: the handler's sleep did not replace backoff's own, so
    the two stacked, and backoff's default full_jitter rewrote Retry-After to
    uniform(0, retry_after), letting the tap retry before the server allowed.
    """

    @staticmethod
    def _waits(exc_factory):
        slept = []
        with patch("time.sleep", side_effect=lambda s: slept.append(s)):
            @RETRY_ON_TRANSIENT
            def always_fails():
                raise exc_factory()
            with suppress(Exception):
                always_fails()
        return slept

    @staticmethod
    def _real_response(status_code, retry_after=None):
        """A genuine requests.Response, not a double.

        requests.Response.__bool__ returns self.ok, so every error response is
        falsy. A `if response:` guard therefore discards the Retry-After header
        on exactly the 429s and 503s it exists to read. The test doubles in this
        module are truthy unless they mirror that, so this asserts against the
        real object.
        """
        resp = requests.Response()
        resp.status_code = status_code
        if retry_after is not None:
            resp.headers["Retry-After"] = str(retry_after)
        return resp

    def test_retry_after_parsed_from_real_falsy_response(self):
        """The header must survive a response object that is falsy."""
        resp = self._real_response(429, 60)
        self.assertFalse(bool(resp))          # documents the trap
        self.assertEqual(ServiceNowRateLimitError("429", resp).retry_after, 60)

    def test_retry_after_parsed_on_5xx_too(self):
        """ServiceNow sends Retry-After on 503 during instance maintenance."""
        resp = self._real_response(503, 120)
        self.assertEqual(
            ServiceNowServiceUnavailableError("503", resp).retry_after, 120
        )

    def test_retry_after_absent_or_unparseable_falls_back(self):
        """No header, or an HTTP-date we do not parse, must not blow up."""
        self.assertIsNone(ServiceNowRateLimitError("429", self._real_response(429)).retry_after)
        bad = self._real_response(429)
        bad.headers["Retry-After"] = "Wed, 21 Oct 2026 07:28:00 GMT"
        self.assertIsNone(ServiceNowRateLimitError("429", bad).retry_after)
        self.assertIsNone(ServiceNowRateLimitError("429", None).retry_after)

    def test_real_429_response_sleeps_exactly_retry_after(self):
        """End-to-end: a real falsy 429 must drive the wait, not expo."""
        waits = self._waits(lambda: ServiceNowRateLimitError("429", self._real_response(429, 60)))
        self.assertEqual(waits, [60.0, 60.0, 60.0, 60.0])

    def test_retry_after_is_honored_exactly(self):
        """A Retry-After of 60 must wait 60 - not 62, and not uniform(0, 60)."""
        def rate_limited():
            exc = ServiceNowRateLimitError("429")
            exc.retry_after = 60
            return exc

        self.assertEqual(self._waits(rate_limited), [60.0, 60.0, 60.0, 60.0])

    def test_retry_after_is_capped(self):
        """A hostile Retry-After cannot park the tap for hours."""
        def rate_limited():
            exc = ServiceNowRateLimitError("429")
            exc.retry_after = 99999
            return exc

        self.assertEqual(
            self._waits(rate_limited), [MAX_BACKOFF_SECONDS] * 4
        )

    def test_expo_fallback_when_no_retry_after(self):
        """Without a Retry-After header, fall back to jittered 2/4/8/16."""
        waits = self._waits(lambda: ConnectionResetError("reset"))
        self.assertEqual(len(waits), 4)
        for wait, bound in zip(waits, [2, 4, 8, 16]):
            self.assertGreaterEqual(wait, 0)
            self.assertLessEqual(wait, bound)

    def test_expo_counter_is_per_generator(self):
        """Interleaved requests must not share the attempt counter.

        Discovery runs the probe across a ThreadPoolExecutor, so a wait_gen
        holding module-level state would let one request's attempt number
        advance another's backoff. backoff builds one generator per decorated
        call, so driving two directly and interleaving their sends is the
        deterministic way to assert that isolation.
        """
        exc = ConnectionResetError("reset")
        gen_a, gen_b = retry_after_or_expo(), retry_after_or_expo()
        next(gen_a)
        next(gen_b)

        # Advance A three steps; B must still be on its own first step.
        bounds_a = [gen_a.send(exc) for _ in range(3)]
        first_b = gen_b.send(exc)

        for wait, bound in zip(bounds_a, [2, 4, 8]):
            self.assertLessEqual(wait, bound)
        # B's first wait is bounded by 2, not by A's fourth step (16).
        self.assertLessEqual(first_b, 2)


class TestDatetimeNormalization(unittest.TestCase):
    """to_snow_dt guards the values that go into sysparm_query.

    An unparseable config or bookmark value used to pass through unchanged,
    producing `sys_updated_on>=<garbage>`. ServiceNow answers that with HTTP
    200 and either zero rows or the condition ignored - silently wrong.
    """

    def test_normalizes_to_servicenow_format(self):
        self.assertEqual(to_snow_dt("2024-02-01T12:34:56Z"), "2024-02-01 12:34:56")

    def test_naive_datetime_treated_as_utc(self):
        self.assertEqual(to_snow_dt("2024-02-01 12:34:56"), "2024-02-01 12:34:56")

    def test_offset_converted_to_utc(self):
        self.assertEqual(to_snow_dt("2024-02-01T14:34:56+02:00"), "2024-02-01 12:34:56")

    def test_strict_raises_on_unparseable_config_value(self):
        with self.assertRaises(InvalidDatetimeError):
            to_snow_dt("not-a-date", strict=True, context="config start_date")

    def test_non_strict_passes_record_data_through_with_a_warning(self):
        """One malformed row must not abort the whole stream."""
        with patch("tap_servicenow.datetime_utils.LOGGER") as mock_log:
            self.assertEqual(to_snow_dt("not-a-date"), "not-a-date")
        mock_log.warning.assert_called_once()

    def test_empty_value_passes_through_in_both_modes(self):
        self.assertEqual(to_snow_dt(""), "")
        self.assertEqual(to_snow_dt("", strict=True), "")

    def test_subsecond_precision_is_truncated_and_warned(self):
        """The keyset cursor's ^NQ branch matches the boundary with `=`.

        Truncating silently would make that clause miss rows inside the same
        second. ServiceNow returns second precision today, so this is a
        tripwire rather than a live defect.
        """
        with patch("tap_servicenow.datetime_utils.LOGGER") as mock_log:
            result = to_snow_dt("2024-02-01T12:34:56.789Z")
        self.assertEqual(result, "2024-02-01 12:34:56")
        mock_log.warning.assert_called_once()
        self.assertIn("sub-second", mock_log.warning.call_args[0][0])

    def test_whole_second_does_not_warn(self):
        with patch("tap_servicenow.datetime_utils.LOGGER") as mock_log:
            to_snow_dt("2024-02-01T12:34:56Z")
        mock_log.warning.assert_not_called()


class TestSessionAuthBinding(unittest.TestCase):
    """Auth must be bound to the Session once, not rewritten per request.

    requests.Session is not documented as thread-safe and the discovery pool
    runs ten threads through it. This fix was silently lost once already - it
    was described in a commit message but absent from the commit - because no
    test covered it.
    """

    def test_auth_is_bound_at_construction(self):
        client = Client(default_config)
        self.assertIsInstance(client._session.auth, requests.auth.HTTPBasicAuth)
        self.assertEqual(client._session.auth.username, default_config["user"])

    def test_authenticate_does_not_rewrite_session_auth(self):
        client = Client(default_config)
        sentinel = object()
        client._session.auth = sentinel
        client.authenticate({}, {})
        self.assertIs(client._session.auth, sentinel,
                      "authenticate() must not touch Session.auth")
