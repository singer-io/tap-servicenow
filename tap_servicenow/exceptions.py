class ServiceNowError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class ServiceNowBackoffError(ServiceNowError):
    """class representing backoff error handling."""
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

class ServiceNowUnprocessableEntityError(ServiceNowBackoffError):
    """class representing 422 status code."""
    pass

class ServiceNowRateLimitError(ServiceNowBackoffError):
    """class representing 429 status code."""
    def __init__(self, message=None, response=None):
        """Initialize the ServiceNowRateLimitError. Parses the 'Retry-After' header from the response (if present) and sets the
            `retry_after` attribute accordingly.
        """
        self.response = response

        # Retry-After header parsing
        retry_after = None
        if response and hasattr(response, 'headers'):
            raw_retry = response.headers.get('Retry-After')
            if raw_retry:
                try:
                    retry_after = int(raw_retry)
                except ValueError:
                    retry_after = None

        self.retry_after = retry_after
        base_msg = message or "Rate limit hit"
        retry_info = f"(Retry after {self.retry_after} seconds.)" \
            if self.retry_after is not None else "(Retry after unknown delay.)"
        full_message = f"{base_msg} {retry_info}"
        super().__init__(full_message, response=response)

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

