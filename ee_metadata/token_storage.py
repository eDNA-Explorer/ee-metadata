"""Secure token storage for eDNA Explorer CLI.

Uses OS keyring (macOS Keychain, Windows Credential Locker, GNOME Keyring)
as the primary storage backend. Falls back to file-based storage only when
explicitly requested with --insecure-storage.
"""

from __future__ import annotations

import contextlib
import json
import os
import stat
import sys
import warnings
from pathlib import Path
from typing import NamedTuple

SERVICE_NAME = "edna-explorer-cli"
ACCOUNT_TOKEN = "token"
ACCOUNT_API_URL = "api_url"


class TokenData(NamedTuple):
    """Token and API URL loaded from storage."""

    token: str
    api_url: str


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _is_headless() -> bool:
    """Detect headless environments (no display server)."""
    if sys.platform == "darwin" or sys.platform == "win32":
        return False
    return not os.environ.get("DISPLAY") and not os.environ.get(
        "WAYLAND_DISPLAY"
    )


def _is_keyring_available() -> bool:
    """Check whether a usable keyring backend is present.

    Returns False for null/fail backends or if keyring is not installed.
    All keyring imports are lazy to avoid a 2s+ startup penalty.
    """
    try:
        import keyring
        from keyring.backends.fail import Keyring as FailKeyring
    except ImportError:
        return False

    try:
        backend = keyring.get_keyring()
    except Exception:
        return False

    # The fail backend means no real backend is configured
    if isinstance(backend, FailKeyring):
        return False

    # Also check for the chainer wrapping only fail backends
    try:
        from keyring.backends.chainer import ChainerBackend

        if isinstance(backend, ChainerBackend):
            viable = [
                b
                for b in backend.backends
                if not isinstance(b, FailKeyring)
            ]
            if not viable:
                return False
    except ImportError:
        pass

    return True


def _config_dir() -> Path:
    """Return platform-appropriate config directory for ee-metadata.

    - Linux: $XDG_CONFIG_HOME/ee-metadata  (default ~/.config/ee-metadata)
    - macOS: ~/Library/Application Support/ee-metadata
    - Windows: %APPDATA%/ee-metadata
    """
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    elif sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / "ee-metadata"


def _token_file() -> Path:
    """Return path to the config-dir token file (insecure fallback)."""
    return _config_dir() / "token.json"


def _legacy_token_file() -> Path:
    """Return path to the legacy token file (read-only, never written)."""
    return Path.home() / ".ednaexplorer" / "token.json"


def _parse_token_json(path: Path) -> TokenData | None:
    """Read and parse a token.json file, returning None on any failure."""
    try:
        data = json.loads(path.read_text())
        token = data.get("token")
        if not token:
            return None
        api_url = data.get("api_url", "https://www.ednaexplorer.org")
        return TokenData(token=token, api_url=api_url)
    except (OSError, json.JSONDecodeError, KeyError):
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_token() -> TokenData | None:
    """Load a token using priority: env var > keyring > config file > legacy file.

    Returns None if no token is found anywhere.
    """
    # 1. Environment variable (highest priority)
    env_token = os.environ.get("EDNA_TOKEN")
    if env_token:
        env_api = os.environ.get("EDNA_API_URL", "https://www.ednaexplorer.org")
        return TokenData(token=env_token, api_url=env_api)

    # 2. Keyring
    if _is_keyring_available():
        import keyring

        try:
            token = keyring.get_password(SERVICE_NAME, ACCOUNT_TOKEN)
            if token:
                api_url = keyring.get_password(SERVICE_NAME, ACCOUNT_API_URL)
                return TokenData(
                    token=token,
                    api_url=api_url or "https://www.ednaexplorer.org",
                )
        except Exception:
            pass

    # 3. Config-dir file
    cfg = _token_file()
    if cfg.exists():
        result = _parse_token_json(cfg)
        if result is not None:
            return result

    # 4. Legacy file (~/.ednaexplorer/token.json)
    legacy = _legacy_token_file()
    if legacy.exists():
        result = _parse_token_json(legacy)
        if result is not None:
            return result

    return None


def store_token(token: str, api_url: str, *, insecure: bool = False) -> str:
    """Store a token securely.

    Tries keyring first. If unavailable and insecure=False, prints guidance
    and exits. If insecure=True, writes to config-dir file with 0600 perms.

    Args:
        token: JWT token string
        api_url: Associated API URL
        insecure: Allow plaintext file storage when keyring is unavailable

    Returns:
        "keyring" or "file" indicating where the token was stored

    Raises:
        SystemExit: If keyring is unavailable and insecure=False
    """
    if _is_keyring_available():
        import keyring

        keyring.set_password(SERVICE_NAME, ACCOUNT_TOKEN, token)
        keyring.set_password(SERVICE_NAME, ACCOUNT_API_URL, api_url)

        # Clean up any existing plaintext files
        for path in (_token_file(), _legacy_token_file()):
            if path.exists():
                with contextlib.suppress(OSError):
                    path.unlink()

        return "keyring"

    if not insecure:
        print(
            "Error: No keyring backend available.\n"
            "\n"
            "ee-metadata requires a system keyring to store tokens securely.\n"
            "\n"
            "Options:\n"
            "  1. Install a keyring backend:\n"
            "     - Linux: sudo apt install gnome-keyring (or kwallet)\n"
            "     - macOS/Windows: keyring should work out of the box\n"
            "  2. Use --insecure-storage to store the token in a plaintext file\n"
            "     (NOT recommended on shared machines)\n",
            file=sys.stderr,
        )
        raise SystemExit(1)

    # Insecure file-based storage
    cfg = _token_file()
    cfg.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    cfg.write_text(json.dumps({"token": token, "api_url": api_url}, indent=2))

    with contextlib.suppress(OSError):
        cfg.chmod(stat.S_IRUSR | stat.S_IWUSR)

    warnings.warn(
        "Token stored in plaintext file. Use a system keyring for better security.",
        UserWarning,
        stacklevel=2,
    )
    return "file"


def clear_token() -> bool:
    """Remove token from all storage locations.

    Returns True if anything was removed, False otherwise.
    """
    removed = False

    # Keyring
    if _is_keyring_available():
        import keyring

        try:
            for account in (ACCOUNT_TOKEN, ACCOUNT_API_URL):
                if keyring.get_password(SERVICE_NAME, account) is not None:
                    keyring.delete_password(SERVICE_NAME, account)
                    removed = True
        except Exception:
            pass

    # Config-dir file and legacy file
    for path in (_token_file(), _legacy_token_file()):
        if path.exists():
            try:
                path.unlink()
                removed = True
            except OSError:
                pass

    return removed


def storage_info() -> dict:
    """Return diagnostic info about the current storage setup."""
    info: dict = {
        "keyring_available": _is_keyring_available(),
        "headless": _is_headless(),
        "storage_method": "none",
        "backend": None,
        "config_dir": str(_config_dir()),
        "token_file": str(_token_file()),
        "legacy_token_file": str(_legacy_token_file()),
    }

    if _is_keyring_available():
        import keyring

        backend = keyring.get_keyring()
        info["backend"] = type(backend).__name__

    # Determine current storage method
    env_token = os.environ.get("EDNA_TOKEN")
    if env_token:
        info["storage_method"] = "environment"
    elif _is_keyring_available():
        import keyring

        try:
            if keyring.get_password(SERVICE_NAME, ACCOUNT_TOKEN):
                info["storage_method"] = "keyring"
        except Exception:
            pass
    if info["storage_method"] == "none":
        if _token_file().exists():
            info["storage_method"] = "file"
        elif _legacy_token_file().exists():
            info["storage_method"] = "legacy_file"

    return info
