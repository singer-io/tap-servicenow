import unittest
from contextlib import suppress

import requests
from unittest.mock import patch
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
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
