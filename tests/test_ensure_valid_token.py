"""Tests for ensure_valid_token() in ee_metadata.main."""

from __future__ import annotations

import base64
import json
import time
import warnings

import pytest

from ee_metadata.main import ensure_valid_token
from ee_metadata.token_storage import TokenData, store_token


def _make_jwt(exp: float) -> str:
    """Create a minimal JWT with the given expiration timestamp."""
    header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode().rstrip("=")
    payload = base64.urlsafe_b64encode(
        json.dumps({"exp": int(exp)}).encode()
    ).decode().rstrip("=")
    return f"{header}.{payload}.fakesig"


def _disable_keyring(monkeypatch):
    """Patch keyring as unavailable in token_storage."""
    monkeypatch.setattr(
        "ee_metadata.token_storage._is_keyring_available", lambda: False
    )


def _use_tmp_token_file(monkeypatch, tmp_path):
    """Redirect token file storage to tmp_path."""
    token_file = tmp_path / "config" / "token.json"
    monkeypatch.setattr(
        "ee_metadata.token_storage._token_file", lambda: token_file
    )
    monkeypatch.setattr(
        "ee_metadata.token_storage._config_dir", lambda: tmp_path / "config"
    )
    return token_file


class TestEnsureValidTokenFileStorage:
    """Verify that token refresh preserves insecure file-based storage."""

    def test_refresh_preserves_file_storage(self, tmp_path, monkeypatch):
        """When keyring is unavailable and token lives in a file,
        refreshing must write back to file without SystemExit."""
        _disable_keyring(monkeypatch)
        token_file = _use_tmp_token_file(monkeypatch, tmp_path)

        # Store an expiring token in file mode
        old_token = _make_jwt(time.time() + 60)  # expires in 60s (<300s threshold)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            store_token(
                old_token, "https://api.test",
                insecure=True, refresh_token="old-rt",
            )

        token_data = TokenData(
            token=old_token,
            api_url="https://api.test",
            refresh_token="old-rt",
        )

        # Mock refresh to return new tokens
        new_token = _make_jwt(time.time() + 7200)
        monkeypatch.setattr(
            "ee_metadata.main.refresh_access_token",
            lambda rt, url: (new_token, "new-rt"),
        )

        # Should NOT raise SystemExit
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = ensure_valid_token(token_data)

        assert result.token == new_token
        assert result.refresh_token == "new-rt"

        # Verify file was updated
        data = json.loads(token_file.read_text())
        assert data["token"] == new_token
        assert data["refresh_token"] == "new-rt"

    def test_refresh_uses_keyring_when_available(self, monkeypatch):
        """When keyring IS available, refresh should store via keyring
        (insecure=False) — the default path."""
        old_token = _make_jwt(time.time() + 60)
        token_data = TokenData(
            token=old_token,
            api_url="https://api.test",
            refresh_token="old-rt",
        )

        new_token = _make_jwt(time.time() + 7200)
        monkeypatch.setattr(
            "ee_metadata.main.refresh_access_token",
            lambda rt, url: (new_token, "new-rt"),
        )

        result = ensure_valid_token(token_data)
        assert result.token == new_token

    def test_no_refresh_when_token_not_expiring(self, monkeypatch):
        """Token with plenty of time left should be returned as-is."""
        token = _make_jwt(time.time() + 7200)  # expires in 2 hours
        token_data = TokenData(
            token=token,
            api_url="https://api.test",
            refresh_token="rt",
        )

        # refresh_access_token should NOT be called
        monkeypatch.setattr(
            "ee_metadata.main.refresh_access_token",
            lambda rt, url: pytest.fail("Should not refresh"),
        )

        result = ensure_valid_token(token_data)
        assert result is token_data
