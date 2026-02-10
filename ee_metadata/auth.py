"""Authentication module for eDNA Explorer CLI."""

import base64
import contextlib
import json
import secrets
import stat
from pathlib import Path
from typing import NamedTuple

import httpx

DEFAULT_API_URL = "https://www.ednaexplorer.org"
TOKEN_DIR = Path.home() / ".ednaexplorer"
TOKEN_FILE = TOKEN_DIR / "token.json"
REQUEST_TIMEOUT = 30.0


class AuthError(Exception):
    """Base exception for authentication errors."""


class TokenNotFoundError(AuthError):
    """Raised when no token file exists."""


class TokenExpiredError(AuthError):
    """Raised when token validation fails with 401."""


class TokenData(NamedTuple):
    """Token and API URL loaded from storage."""

    token: str
    api_url: str


class UserInfo(NamedTuple):
    """User information returned from API."""

    id: str
    email: str
    name: str


def get_token_path() -> Path:
    """Return the path to the token file."""
    return TOKEN_FILE


def save_token(token: str, api_url: str) -> None:
    """Save token to disk with secure permissions.

    Creates ~/.ednaexplorer directory if it doesn't exist.
    Sets file permissions to 0600 (owner read/write only).

    Args:
        token: The JWT token to save
        api_url: The API URL associated with this token

    Raises:
        OSError: If unable to create directory or write file
    """
    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps({"token": token, "api_url": api_url}, indent=2))

    # Set secure permissions (owner read/write only)
    # On Windows, this may not work the same way but won't raise
    with contextlib.suppress(OSError):
        TOKEN_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)


def load_token() -> TokenData:
    """Load token from disk.

    Returns:
        TokenData containing token and api_url

    Raises:
        TokenNotFoundError: If token file doesn't exist
        AuthError: If token file is invalid/corrupted
    """
    if not TOKEN_FILE.exists():
        raise TokenNotFoundError(
            "Not logged in. Run 'ee-metadata login' to authenticate."
        )

    try:
        data = json.loads(TOKEN_FILE.read_text())
        token = data.get("token")
        api_url = data.get("api_url", DEFAULT_API_URL)

        if not token:
            raise AuthError("Token file is corrupted (missing token).")

        return TokenData(token=token, api_url=api_url)

    except json.JSONDecodeError as e:
        raise AuthError(f"Token file is corrupted: {e}") from e


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

        if response.status_code != 200:
            raise AuthError(
                f"API returned status {response.status_code}: {response.text}"
            )

        data = response.json()
        return UserInfo(
            id=data.get("id", ""),
            email=data.get("email", ""),
            name=data.get("name", ""),
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


def clear_token() -> bool:
    """Delete the token file.

    Returns:
        True if token was deleted, False if no token existed
    """
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
        return True
    return False
