"""Authentication module for eDNA Explorer CLI."""

from __future__ import annotations

import base64
import json
import secrets
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import NamedTuple
from urllib.parse import parse_qs, urlparse

import httpx

DEFAULT_API_URL = "https://www.ednaexplorer.org"
REQUEST_TIMEOUT = 30.0
REFRESH_THRESHOLD_SECONDS = 300  # 5 minutes


def _clean_response_body(response: httpx.Response, max_len: int = 200) -> str:
    """Extract a clean error snippet from an HTTP response.

    If the response looks like HTML (e.g. a Next.js 404 page), return a short
    message instead of dumping the entire markup to the terminal.
    """
    text = response.text.strip()
    content_type = response.headers.get("content-type", "")

    if "text/html" in content_type or text[:15].lower().startswith(("<!doctype", "<html")):
        return "(server returned an HTML error page)"

    if len(text) > max_len:
        return text[:max_len] + "…"
    return text


class AuthError(Exception):
    """Base exception for authentication errors."""


class TokenExpiredError(AuthError):
    """Raised when token validation fails with 401."""


class UserInfo(NamedTuple):
    """User information returned from API."""

    id: str
    email: str
    name: str
    role: str


class DeviceCodeResponse(NamedTuple):
    """Response from the device code request endpoint."""

    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    expires_in: int
    interval: int


def validate_token(token: str, api_url: str) -> UserInfo:
    """Validate token by calling the API.

    Args:
        token: JWT token to validate
        api_url: Base URL of the API

    Returns:
        UserInfo with id, email, and name

    Raises:
        TokenExpiredError: If token is invalid or expired (401)
        AuthError: If API call fails for other reasons
    """
    url = f"{api_url.rstrip('/')}/api/cli/me"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            response = client.get(url, headers=headers)

        if response.status_code == 401:
            raise TokenExpiredError(
                "Token is invalid or expired. Run 'ee-metadata login' again."
            )

        if response.status_code == 404:
            raise AuthError(
                "Authentication endpoint not found. "
                "The API URL may be incorrect or the server may be outdated."
            )

        if response.status_code != 200:
            raise AuthError(
                f"API returned status {response.status_code}: "
                f"{_clean_response_body(response)}"
            )

        data = response.json()
        return UserInfo(
            id=data.get("id", ""),
            email=data.get("email", ""),
            name=data.get("name", ""),
            role=data.get("role", "USER"),
        )

    except httpx.TimeoutException as e:
        raise AuthError(f"Request timed out connecting to {api_url}") from e
    except httpx.RequestError as e:
        raise AuthError(f"Failed to connect to {api_url}: {e}") from e


def generate_state() -> str:
    """Generate a random state string for CSRF protection."""
    return secrets.token_urlsafe(32)


def decode_token_claims(token: str) -> dict:
    """Decode JWT payload without verifying signature.

    Used to extract state claim before server-side validation.

    NOTE: This is a UX guard (detecting wrong-session tokens or paste errors),
    NOT a security boundary. The actual signature verification happens
    server-side in validate_token(). Do not rely on these claims for
    authorization decisions.

    Args:
        token: JWT token string

    Returns:
        Dictionary of claims from the JWT payload

    Raises:
        AuthError: If token format is invalid
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise AuthError("Invalid token format")

    # Decode payload (add padding if needed for base64)
    payload = parts[1]
    padding = 4 - len(payload) % 4
    if padding != 4:
        payload += "=" * padding

    try:
        decoded = base64.urlsafe_b64decode(payload)
        return json.loads(decoded)
    except (ValueError, json.JSONDecodeError) as e:
        raise AuthError(f"Invalid token payload: {e}") from e


def is_token_expiring_soon(
    token: str, threshold: int = REFRESH_THRESHOLD_SECONDS
) -> bool:
    """Check if JWT expires within threshold seconds.

    Uses decode_token_claims (UX guard, not security).
    """
    try:
        claims = decode_token_claims(token)
        exp = claims.get("exp")
        if exp is None:
            return False
        return time.time() >= (exp - threshold)
    except AuthError:
        return True


def refresh_access_token(refresh_token: str, api_url: str) -> tuple[str, str]:
    """Exchange a refresh token for a new access token and refresh token.

    Args:
        refresh_token: The refresh token from previous login/refresh
        api_url: Base URL of the API

    Returns:
        Tuple of (new_access_token, new_refresh_token)

    Raises:
        TokenExpiredError: If the refresh token is invalid or expired (401)
        AuthError: If the request fails for other reasons
    """
    url = f"{api_url.rstrip('/')}/api/cli/refresh"

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            response = client.post(url, json={"refresh_token": refresh_token})
    except httpx.TimeoutException as e:
        raise AuthError(f"Request timed out connecting to {api_url}") from e
    except httpx.RequestError as e:
        raise AuthError(f"Failed to connect to {api_url}: {e}") from e

    if response.status_code == 401:
        raise TokenExpiredError(
            "Refresh token expired. Run 'ee-metadata login' again."
        )

    if response.status_code != 200:
        raise AuthError(
            f"Token refresh failed ({response.status_code}): "
            f"{_clean_response_body(response)}"
        )

    data = response.json()
    new_token = data.get("token")
    new_refresh = data.get("refresh_token")
    if not new_token or not new_refresh:
        raise AuthError("Token refresh returned incomplete response.")

    return new_token, new_refresh


# =============================================================================
# Browser-based login (local callback server)
# =============================================================================

CALLBACK_TIMEOUT = 300.0  # 5 minutes


class CallbackResult(NamedTuple):
    """Result from the local callback server."""

    code: str
    state: str


class _CallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler for the OAuth callback from the browser."""

    server: _CallbackServer

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return

        params = parse_qs(parsed.query)
        self.server.callback_code = params.get("code", [None])[0]
        self.server.callback_state = params.get("state", [None])[0]

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        html = (
            "<html><body style='font-family:system-ui;display:flex;"
            "justify-content:center;align-items:center;height:100vh;margin:0'>"
            "<div style='text-align:center'>"
            "<h1 style='color:#16a34a'>&#10003; Authentication successful</h1>"
            "<p style='color:#6b7280'>You can close this tab and return to the terminal.</p>"
            "</div></body></html>"
        )
        self.wfile.write(html.encode())
        self.server.callback_received.set()

    def log_message(self, format: str, *args: object) -> None:
        """Suppress default HTTP server logging."""


class _CallbackServer(HTTPServer):
    """HTTP server that waits for a single OAuth callback."""

    def __init__(self, port: int = 0) -> None:
        super().__init__(("127.0.0.1", port), _CallbackHandler)
        self.callback_code: str | None = None
        self.callback_state: str | None = None
        self.callback_received = threading.Event()


def start_callback_server() -> tuple[_CallbackServer, int]:
    """Create and bind the local callback server.

    Returns:
        Tuple of (server instance, port number)

    Raises:
        OSError: If unable to bind to any port on localhost
    """
    server = _CallbackServer(port=0)
    port = server.server_address[1]
    return server, port


def wait_for_callback(
    server: _CallbackServer,
    timeout: float = CALLBACK_TIMEOUT,
) -> CallbackResult | None:
    """Run the callback server and wait for the browser redirect.

    Args:
        server: The callback server from start_callback_server()
        timeout: Maximum seconds to wait (default: 5 minutes)

    Returns:
        CallbackResult if callback received, None if timed out
    """
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    try:
        got_callback = server.callback_received.wait(timeout=timeout)
        if not got_callback or not server.callback_code:
            return None
        return CallbackResult(
            code=server.callback_code,
            state=server.callback_state or "",
        )
    finally:
        server.shutdown()


class ExchangeResult(NamedTuple):
    """Result from code exchange."""

    token: str
    refresh_token: str | None


def exchange_code(code: str, api_url: str) -> ExchangeResult:
    """Exchange a short-lived authorization code for a CLI token.

    Args:
        code: The authorization code from the browser callback
        api_url: Base URL of the API

    Returns:
        ExchangeResult with access token and optional refresh token

    Raises:
        AuthError: If the exchange fails
    """
    url = f"{api_url.rstrip('/')}/api/cli/exchange"

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            response = client.post(url, json={"code": code})

        if response.status_code != 200:
            raise AuthError(
                f"Code exchange failed ({response.status_code}): "
                f"{_clean_response_body(response)}"
            )

        data = response.json()
        token = data.get("token")
        if not token:
            raise AuthError("Code exchange returned empty token.")
        refresh_token = data.get("refresh_token")

    except httpx.TimeoutException as e:
        raise AuthError(f"Request timed out connecting to {api_url}") from e
    except httpx.RequestError as e:
        raise AuthError(f"Failed to connect to {api_url}: {e}") from e

    return ExchangeResult(token=token, refresh_token=refresh_token)


# =============================================================================
# Device Authorization Flow (RFC 8628)
# =============================================================================


def request_device_code(api_url: str) -> DeviceCodeResponse:
    """Request a device code for the device authorization flow.

    Args:
        api_url: Base URL of the API

    Returns:
        DeviceCodeResponse with device_code, user_code, and polling parameters

    Raises:
        AuthError: If the request fails
    """
    url = f"{api_url.rstrip('/')}/api/cli/device/code"

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            response = client.post(url)

        if response.status_code != 200:
            raise AuthError(
                f"Device code request failed ({response.status_code}): "
                f"{_clean_response_body(response)}"
            )

        data = response.json()
        return DeviceCodeResponse(
            device_code=data["device_code"],
            user_code=data["user_code"],
            verification_uri=data["verification_uri"],
            verification_uri_complete=data["verification_uri_complete"],
            expires_in=data["expires_in"],
            interval=data["interval"],
        )

    except KeyError as e:
        raise AuthError(f"Device code response missing field: {e}") from e
    except httpx.TimeoutException as e:
        raise AuthError(f"Request timed out connecting to {api_url}") from e
    except httpx.RequestError as e:
        raise AuthError(f"Failed to connect to {api_url}: {e}") from e


def poll_device_token(
    device_code: str, api_url: str, interval: int, expires_in: int
) -> ExchangeResult:
    """Poll for the device token until the user authorizes or the code expires.

    Args:
        device_code: The device code from request_device_code()
        api_url: Base URL of the API
        interval: Initial polling interval in seconds
        expires_in: Maximum time to poll in seconds

    Returns:
        ExchangeResult with access token and optional refresh token

    Raises:
        AuthError: If the code expires, is denied, or a network error occurs
    """
    url = f"{api_url.rstrip('/')}/api/cli/device/token"
    start = time.monotonic()
    poll_interval = interval

    while True:
        time.sleep(poll_interval)

        elapsed = time.monotonic() - start
        if elapsed >= expires_in:
            raise AuthError(
                "Device code expired. Please run the login command again."
            )

        try:
            with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
                response = client.post(url, json={"device_code": device_code})

            if response.status_code == 200:
                data = response.json()
                token = data.get("token")
                if not token:
                    raise AuthError("Device token response missing token.")
                refresh_token = data.get("refresh_token")
                return ExchangeResult(token=token, refresh_token=refresh_token)

            # Handle pending/error states per RFC 8628
            data = response.json()
            error = data.get("error", "")

            if error == "authorization_pending":
                continue
            elif error == "slow_down":
                poll_interval += 5
                continue
            elif error == "expired_token":
                raise AuthError(
                    "Device code expired. Please run the login command again."
                )
            elif error == "access_denied":
                raise AuthError("Authorization was denied by the user.")
            else:
                raise AuthError(
                    f"Device token request failed ({response.status_code}): "
                    f"{_clean_response_body(response)}"
                )

        except httpx.TimeoutException as e:
            raise AuthError(f"Request timed out connecting to {api_url}") from e
        except httpx.RequestError as e:
            raise AuthError(f"Failed to connect to {api_url}: {e}") from e


def open_browser(url: str) -> bool:
    """Open URL in the default browser.

    Returns:
        True if the browser was opened, False otherwise.
    """
    try:
        return webbrowser.open(url)
    except Exception:
        return False
