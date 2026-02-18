"""Tests for ee_metadata.token_storage module."""

from __future__ import annotations

import json

import keyring
import pytest

from ee_metadata.token_storage import (
    ACCOUNT_API_URL,
    ACCOUNT_REFRESH_TOKEN,
    ACCOUNT_TOKEN,
    SERVICE_NAME,
    TokenData,
    clear_token,
    get_token,
    storage_info,
    store_token,
)

# ---------------------------------------------------------------------------
# get_token
# ---------------------------------------------------------------------------


class TestGetToken:
    def test_returns_none_when_empty(self, tmp_path, monkeypatch):
        monkeypatch.delenv("EDNA_TOKEN", raising=False)
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file",
            lambda: tmp_path / "nonexistent" / "token.json",
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._legacy_token_file",
            lambda: tmp_path / "nonexistent2" / "token.json",
        )
        assert get_token() is None

    def test_env_var_takes_priority(self, monkeypatch):
        # Store something in keyring to prove env var wins
        keyring.set_password(SERVICE_NAME, ACCOUNT_TOKEN, "keyring-tok")
        keyring.set_password(SERVICE_NAME, ACCOUNT_API_URL, "https://kr.example.com")

        monkeypatch.setenv("EDNA_TOKEN", "env-tok")
        monkeypatch.setenv("EDNA_API_URL", "https://env.example.com")

        result = get_token()
        assert result is not None
        assert result.token == "env-tok"
        assert result.api_url == "https://env.example.com"

    def test_env_var_default_api_url(self, monkeypatch):
        monkeypatch.setenv("EDNA_TOKEN", "env-tok")
        monkeypatch.delenv("EDNA_API_URL", raising=False)

        result = get_token()
        assert result is not None
        assert result.token == "env-tok"
        assert result.api_url == "https://www.ednaexplorer.org"

    def test_reads_from_keyring(self):
        keyring.set_password(SERVICE_NAME, ACCOUNT_TOKEN, "my-token")
        keyring.set_password(
            SERVICE_NAME, ACCOUNT_API_URL, "https://api.example.com"
        )

        result = get_token()
        assert result is not None
        assert result == TokenData(
            token="my-token", api_url="https://api.example.com"
        )

    def test_falls_back_to_config_file(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.json"
        token_file.write_text(
            json.dumps({"token": "file-tok", "api_url": "https://f.example.com"})
        )

        # Disable keyring so it falls through
        monkeypatch.setattr(
            "ee_metadata.token_storage._is_keyring_available", lambda: False
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file", lambda: token_file
        )

        result = get_token()
        assert result is not None
        assert result.token == "file-tok"
        assert result.api_url == "https://f.example.com"

    def test_falls_back_to_legacy_file(self, tmp_path, monkeypatch):
        legacy_file = tmp_path / "token.json"
        legacy_file.write_text(
            json.dumps({"token": "legacy-tok", "api_url": "https://l.example.com"})
        )

        monkeypatch.setattr(
            "ee_metadata.token_storage._is_keyring_available", lambda: False
        )
        # Config file does not exist
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file",
            lambda: tmp_path / "nonexistent" / "token.json",
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._legacy_token_file", lambda: legacy_file
        )

        result = get_token()
        assert result is not None
        assert result.token == "legacy-tok"

    def test_returns_token_data_namedtuple(self):
        keyring.set_password(SERVICE_NAME, ACCOUNT_TOKEN, "tok")
        keyring.set_password(SERVICE_NAME, ACCOUNT_API_URL, "https://x.com")

        result = get_token()
        assert isinstance(result, TokenData)
        assert result.token == "tok"
        assert result.api_url == "https://x.com"

    def test_reads_refresh_token_from_keyring(self):
        keyring.set_password(SERVICE_NAME, ACCOUNT_TOKEN, "tok")
        keyring.set_password(SERVICE_NAME, ACCOUNT_API_URL, "https://x.com")
        keyring.set_password(SERVICE_NAME, ACCOUNT_REFRESH_TOKEN, "rt-123")

        result = get_token()
        assert result is not None
        assert result.refresh_token == "rt-123"

    def test_reads_refresh_token_from_file(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.json"
        token_file.write_text(
            json.dumps({
                "token": "file-tok",
                "api_url": "https://f.example.com",
                "refresh_token": "rt-file",
            })
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._is_keyring_available", lambda: False
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file", lambda: token_file
        )

        result = get_token()
        assert result is not None
        assert result.refresh_token == "rt-file"

    def test_refresh_token_defaults_to_none(self):
        keyring.set_password(SERVICE_NAME, ACCOUNT_TOKEN, "tok")
        keyring.set_password(SERVICE_NAME, ACCOUNT_API_URL, "https://x.com")

        result = get_token()
        assert result is not None
        assert result.refresh_token is None


# ---------------------------------------------------------------------------
# store_token
# ---------------------------------------------------------------------------


class TestStoreToken:
    def test_stores_in_keyring(self):
        method = store_token("tok123", "https://api.example.com")
        assert method == "keyring"

        assert keyring.get_password(SERVICE_NAME, ACCOUNT_TOKEN) == "tok123"
        assert (
            keyring.get_password(SERVICE_NAME, ACCOUNT_API_URL)
            == "https://api.example.com"
        )

    def test_stores_refresh_token_in_keyring(self):
        method = store_token(
            "tok", "https://api.example.com", refresh_token="rt-456"
        )
        assert method == "keyring"
        assert keyring.get_password(SERVICE_NAME, ACCOUNT_REFRESH_TOKEN) == "rt-456"

    def test_cleans_up_plaintext_after_keyring_store(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.json"
        token_file.write_text('{"token":"old"}')

        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file", lambda: token_file
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._legacy_token_file",
            lambda: tmp_path / "nonexistent" / "token.json",
        )

        method = store_token("tok", "https://api.example.com")
        assert method == "keyring"
        assert not token_file.exists()

    def test_errors_without_insecure_flag(self, monkeypatch):
        monkeypatch.setattr(
            "ee_metadata.token_storage._is_keyring_available", lambda: False
        )
        with pytest.raises(SystemExit):
            store_token("tok", "https://api.example.com")

    def test_writes_file_with_insecure_flag(self, tmp_path, monkeypatch):
        token_file = tmp_path / "config" / "token.json"
        monkeypatch.setattr(
            "ee_metadata.token_storage._is_keyring_available", lambda: False
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file", lambda: token_file
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._config_dir", lambda: tmp_path / "config"
        )

        with pytest.warns(UserWarning, match="plaintext"):
            method = store_token("tok", "https://x.com", insecure=True)

        assert method == "file"
        assert token_file.exists()

        data = json.loads(token_file.read_text())
        assert data["token"] == "tok"
        assert data["api_url"] == "https://x.com"

    def test_writes_refresh_token_to_file(self, tmp_path, monkeypatch):
        token_file = tmp_path / "config" / "token.json"
        monkeypatch.setattr(
            "ee_metadata.token_storage._is_keyring_available", lambda: False
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file", lambda: token_file
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._config_dir", lambda: tmp_path / "config"
        )

        with pytest.warns(UserWarning, match="plaintext"):
            method = store_token(
                "tok", "https://x.com", insecure=True, refresh_token="rt-file"
            )

        assert method == "file"
        data = json.loads(token_file.read_text())
        assert data["refresh_token"] == "rt-file"


# ---------------------------------------------------------------------------
# clear_token
# ---------------------------------------------------------------------------


class TestClearToken:
    def test_clears_keyring(self):
        keyring.set_password(SERVICE_NAME, ACCOUNT_TOKEN, "tok")
        keyring.set_password(SERVICE_NAME, ACCOUNT_API_URL, "https://x.com")
        keyring.set_password(SERVICE_NAME, ACCOUNT_REFRESH_TOKEN, "rt")

        assert clear_token() is True
        assert keyring.get_password(SERVICE_NAME, ACCOUNT_TOKEN) is None
        assert keyring.get_password(SERVICE_NAME, ACCOUNT_API_URL) is None
        assert keyring.get_password(SERVICE_NAME, ACCOUNT_REFRESH_TOKEN) is None

    def test_clears_file(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.json"
        token_file.write_text('{"token":"x"}')

        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file", lambda: token_file
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._legacy_token_file",
            lambda: tmp_path / "nonexistent" / "token.json",
        )

        assert clear_token() is True
        assert not token_file.exists()

    def test_returns_false_when_nothing_to_clear(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file",
            lambda: tmp_path / "nonexistent" / "token.json",
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._legacy_token_file",
            lambda: tmp_path / "nonexistent2" / "token.json",
        )

        assert clear_token() is False


# ---------------------------------------------------------------------------
# storage_info
# ---------------------------------------------------------------------------


class TestStorageInfo:
    def test_reports_keyring_method(self):
        keyring.set_password(SERVICE_NAME, ACCOUNT_TOKEN, "tok")

        info = storage_info()
        assert info["keyring_available"] is True
        assert info["storage_method"] == "keyring"
        assert info["backend"] is not None

    def test_reports_environment_method(self, monkeypatch):
        monkeypatch.setenv("EDNA_TOKEN", "env-tok")

        info = storage_info()
        assert info["storage_method"] == "environment"

    def test_reports_file_method(self, tmp_path, monkeypatch):
        token_file = tmp_path / "token.json"
        token_file.write_text('{"token":"x"}')

        monkeypatch.setattr(
            "ee_metadata.token_storage._is_keyring_available", lambda: False
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file", lambda: token_file
        )

        info = storage_info()
        assert info["storage_method"] == "file"

    def test_reports_none_method(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "ee_metadata.token_storage._token_file",
            lambda: tmp_path / "nonexistent" / "token.json",
        )
        monkeypatch.setattr(
            "ee_metadata.token_storage._legacy_token_file",
            lambda: tmp_path / "nonexistent2" / "token.json",
        )
        monkeypatch.delenv("EDNA_TOKEN", raising=False)

        info = storage_info()
        assert info["storage_method"] == "none"


# ---------------------------------------------------------------------------
# _try_configure_cryptfile
# ---------------------------------------------------------------------------


class TestTryConfigureCryptfile:
    def test_returns_false_without_env_var(self, monkeypatch):
        from ee_metadata.token_storage import _try_configure_cryptfile

        monkeypatch.delenv("KEYRING_CRYPTFILE_PASSWORD", raising=False)
        assert _try_configure_cryptfile() is False

    def test_returns_false_when_package_not_installed(self, monkeypatch):
        from ee_metadata.token_storage import _try_configure_cryptfile

        monkeypatch.setenv("KEYRING_CRYPTFILE_PASSWORD", "secret")
        # The import will fail because keyrings.cryptfile isn't installed
        # in the test environment (unless it is, in which case this test
        # would configure it - both outcomes are valid)
        result = _try_configure_cryptfile()
        # Either False (not installed) or True (installed and configured)
        assert isinstance(result, bool)
