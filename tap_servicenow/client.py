from typing import Any, Dict, Mapping, Optional, Tuple

import random

import backoff
import requests
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger, metrics
from requests.auth import HTTPBasicAuth

from tap_servicenow.exceptions import ERROR_CODE_EXCEPTION_MAPPING, ServiceNowError, ServiceNowBackoffError

LOGGER = get_logger()
REQUEST_TIMEOUT = 300

def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
        else:
            error_message = ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}
            ).get("message", "Unknown Error")
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('message', error_message)}"
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
            "raise_exception", ServiceNowError
        )
        raise exc(message, response) from None

MAX_BACKOFF_SECONDS = 300


def retry_after_or_expo(base: float = 2, factor: float = 2,
                        max_value: float = MAX_BACKOFF_SECONDS):
    """wait_gen honoring ServiceNow's Retry-After, falling back to exponential.

    Yields Retry-After unchanged when the response carried one (429s, and 5xx
    responses that include the header). Otherwise yields the same exponential
    schedule as before - 2, 4, 8, 16 seconds - with full jitter applied, for
    connection resets, timeouts, and 5xx without a header.

    Two things this has to get right, both of which the previous
    on_backoff-based version got wrong:

    1. Sleeping inside on_backoff does not replace backoff's own sleep.
       `retry_exception` passes the generated wait to the handler by keyword
       and then sleeps its own local copy, so the two stacked: a 429 with
       `Retry-After: 60` waited 62, 64, 68, then 76 seconds. Mutating
       details['wait'] from the handler does not help either, for the same
       reason - the sleep never reads it back. Yielding the value here makes
       it the wait, exactly once.

    2. Jitter must not apply to Retry-After. backoff's default `full_jitter`
       rewrites any wait to uniform(0, wait), which turns a 60-second
       Retry-After into anything from 0 to 60 and lets the tap retry well
       before the server said it may. The decorator therefore passes
       jitter=None, and this generator jitters only the exponential branch,
       where spreading retries across the discovery thread pool is what we
       actually want.

    backoff builds a fresh generator per decorated call, so `attempt` is
    per-request and safe under that thread pool.
    """
    exc = yield
    attempt = 0
    while True:
        retry_after = getattr(exc, "retry_after", None)
        if retry_after is not None:
            # Honor the server's instruction exactly; no jitter, no expo.
            wait = min(float(retry_after), max_value)
        else:
            wait = random.uniform(0, min(factor * base ** attempt, max_value))
            attempt += 1
        exc = yield wait


# Shared retry policy for ALL outbound requests. Retries transient failures
# (connection resets, timeouts) and ServiceNowBackoffError (429 / 5xx, which
# carries Retry-After). 401/403/404 are intentionally absent, so they raise
# immediately for the caller to handle. Both make_request() and the get() access
# probe use this - previously get() had no retry, so a single 429 during
# discovery silently dropped a table the account could actually read.
# jitter=None because retry_after_or_expo applies jitter itself, only on the
# branch where it is appropriate (see its docstring).
RETRY_ON_TRANSIENT = backoff.on_exception(
    wait_gen=retry_after_or_expo,
    jitter=None,
    exception=(
        ConnectionResetError,
        ConnectionError,
        ChunkedEncodingError,
        Timeout,
        ServiceNowBackoffError,  # covers ServiceNowRateLimitError via inheritance
    ),
    max_tries=5,
)


class Client:
    """
    A Wrapper class.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = f"https://{config['instance']}.service-now.com/api/now/table"
        config_request_timeout = config.get("request_timeout")
        self.request_timeout = float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        pass

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Authenticates the request with basic auth headers."""
        self._session.auth = HTTPBasicAuth(
            self.config["user"],
            self.config["password"]
        )
        return headers, params

    @RETRY_ON_TRANSIENT
    def get_total_count(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """Return the total record count from the X-Total-Count response header.

        Makes a lightweight probe request (sysparm_limit=1, no sysparm_no_count)
        so ServiceNow includes X-Total-Count in the response.  This value
        reflects the table size BEFORE row-level ACL filtering, letting
        get_records() paginate through ACL-hidden rows instead of stopping on
        the first empty page.

        Returns None when the header is absent (e.g. on virtual tables).
        """
        probe_params = dict(params or {})
        probe_params.pop("sysparm_no_count", None)  # must be absent for the header
        probe_params["sysparm_limit"] = 1
        probe_params["sysparm_offset"] = 0
        probe_headers = dict(headers or {})
        probe_headers, probe_params = self.authenticate(probe_headers, probe_params)
        response = self._session.get(
            endpoint, headers=probe_headers, params=probe_params,
            timeout=self.request_timeout,
        )
        raise_for_error(response)
        count_str = response.headers.get("X-Total-Count", "")
        return int(count_str) if count_str.isdigit() else None

    @RETRY_ON_TRANSIENT
    def get(
        self,
        table: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None
    ) -> None:
        """Per-table read-access probe: raises a ServiceNowError subclass on 4xx/5xx.

        Routes through raise_for_error so callers receive typed exceptions
        (e.g. ServiceNowForbiddenError) rather than raw status codes. The shared
        RETRY_ON_TRANSIENT policy retries 429/5xx (honoring Retry-After) so a
        transient rate-limit during the discovery probe does NOT wrongly drop a
        table the account can actually read; 401/403/404 raise immediately for
        the caller to classify.
        """
        params = params or {}
        headers = headers or {}
        headers, params = self.authenticate(headers, params)
        url = f"{self.base_url}/{table}"
        response = self._session.get(url, headers=headers, params=params, timeout=self.request_timeout)
        raise_for_error(response)

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None
    ) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        """
        params = params or {}
        headers = headers or {}
        body = body or {}
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request(
            method, endpoint,
            headers=headers,
            params=params,
            data=body,
            timeout=self.request_timeout
        )

    @RETRY_ON_TRANSIENT
    def __make_request(
        self, method: str, endpoint: str, **kwargs
    ) -> Optional[Mapping[Any, Any]]:
        """Performs HTTP Operations."""
        method = method.upper()
        with metrics.http_request_timer(endpoint):
            if method in ("GET", "POST"):
                if method == "GET":
                    kwargs.pop("data", None)
                response = self._session.request(method, endpoint, **kwargs)
                raise_for_error(response)
            else:
                raise ValueError(f"Unsupported method: {method}")

        return response.json()

