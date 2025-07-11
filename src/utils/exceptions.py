class TikTokException(Exception):
    """Generic exception that all other TikTok errors are children of."""

    def __init__(self, message, error_code=None):
        self.error_code = error_code
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.error_code} -> {self.message}"


class AuthenticationError(TikTokException):
    """Failing to establish authentication"""


class BadRequestError(TikTokException):
    """Bad request made"""
