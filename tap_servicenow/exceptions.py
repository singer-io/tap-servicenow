class ServiceNowError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class ServiceNowBackoffError(ServiceNowError):
    """Base for retryable errors; parses the Retry-After header if present.

    ServiceNow sends Retry-After on 429 and also on 503 during instance
    maintenance, so the parsing lives here rather than on the 429 subclass.
    """

    def __init__(self, message=None, response=None):
        self.retry_after = _parse_retry_after(response)
        super().__init__(message, response=response)


def _parse_retry_after(response):
    """Seconds from a Retry-After header, or None.

    `response is not None` matters: requests.Response.__bool__ returns
    self.ok, so every error response is falsy. A truthiness check here
    silently discarded the header on exactly the 429s and 503s it exists to
    read, and the tap fell back to exponential backoff while claiming to
    honor the server's instruction.
    """
    headers = getattr(response, "headers", None)
    if not headers:
        return None
    raw_retry = headers.get("Retry-After")
    if not raw_retry:
        return None
    try:
        # Retry-After may also be an HTTP-date, which we do not parse; falling
        # back to exponential is correct there.
        return int(raw_retry)
    except (TypeError, ValueError):
        return None

class ServiceNowIncompleteSyncError(ServiceNowError):
    """A stream stopped before reaching the end of its data.

    Raised when the keyset cursor cannot advance while pages remain, so the
    tap has no way to reach the rest of the table. Not an HTTP error: the
    requests succeeded, which is what makes it dangerous - without this the
    stream returns a short record set that looks like a completed sync.
    """
    pass


class ServiceNowBadRequestError(ServiceNowError):
    """class representing 400 status code."""
    pass

class ServiceNowUnauthorizedError(ServiceNowError):
    """class representing 401 status code."""
    pass


class ServiceNowForbiddenError(ServiceNowError):
    """class representing 403 status code."""
    pass

class ServiceNowNotFoundError(ServiceNowError):
    """class representing 404 status code."""
    pass

class ServiceNowConflictError(ServiceNowError):
    """class representing 409 status code."""
    pass

class ServiceNowUnprocessableEntityError(ServiceNowError):
    """class representing 422 status code.

    422 Unprocessable Entity means the request syntax was valid but the
    content is semantically incorrect (e.g. a bad sysparm_query string).
    Retrying the same request will always fail, so this must NOT inherit
    from ServiceNowBackoffError.
    """
    pass

class ServiceNowRateLimitError(ServiceNowBackoffError):
    """class representing 429 status code."""
    def __init__(self, message=None, response=None):
        """Annotates the message with the Retry-After delay when one was sent."""
        retry_after = _parse_retry_after(response)
        base_msg = message or "Rate limit hit"
        retry_info = f"(Retry after {retry_after} seconds.)" \
            if retry_after is not None else "(Retry after unknown delay.)"
        super().__init__(f"{base_msg} {retry_info}", response=response)

class ServiceNowInternalServerError(ServiceNowBackoffError):
    """class representing 500 status code."""
    pass

class ServiceNowNotImplementedError(ServiceNowBackoffError):
    """class representing 501 status code."""
    pass

class ServiceNowBadGatewayError(ServiceNowBackoffError):
    """class representing 502 status code."""
    pass

class ServiceNowServiceUnavailableError(ServiceNowBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": ServiceNowBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": ServiceNowUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": ServiceNowForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": ServiceNowNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": ServiceNowConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": ServiceNowUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": ServiceNowRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": ServiceNowInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": ServiceNowNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": ServiceNowBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": ServiceNowServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}

