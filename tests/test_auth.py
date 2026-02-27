"""Tests for the device authorization flow and refresh logic in auth module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from ee_metadata.auth import (
    AuthError,
    DeviceCodeResponse,
    ExchangeResult,
    TokenExpiredError,
    is_token_expiring_soon,
    poll_device_token,
    refresh_access_token,
    request_device_code,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

API_URL = "https://example.com"

DEVICE_CODE_RESPONSE = {
    "device_code": "dev-abc123",
    "user_code": "KVLP-QMMQ",
    "verification_uri": "https://example.com/cli/device",
    "verification_uri_complete": "https://example.com/cli/device?code=KVLP-QMMQ",
    "expires_in": 600,
    "interval": 5,
}


def _make_response(status_code: int, json_data: dict) -> httpx.Response:
    """Build a fake httpx.Response."""
    resp = httpx.Response(status_code=status_code, json=json_data)
    return resp


# ---------------------------------------------------------------------------
# request_device_code
# ---------------------------------------------------------------------------


class TestRequestDeviceCode:
    def test_success(self):
        mock_response = _make_response(200, DEVICE_CODE_RESPONSE)

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(return_value=mock_response)

            result = request_device_code(API_URL)

        assert isinstance(result, DeviceCodeResponse)
        assert result.device_code == "dev-abc123"
        assert result.user_code == "KVLP-QMMQ"
        assert result.verification_uri == "https://example.com/cli/device"
        assert result.expires_in == 600
        assert result.interval == 5

        MockClient.return_value.post.assert_called_once_with(
            f"{API_URL}/api/cli/device/code"
        )

    def test_server_error_raises_auth_error(self):
        mock_response = _make_response(500, {"error": "internal"})

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(return_value=mock_response)

            with pytest.raises(AuthError, match="Device code request failed"):
                request_device_code(API_URL)

    def test_timeout_raises_auth_error(self):
        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(
                side_effect=httpx.TimeoutException("timed out")
            )

            with pytest.raises(AuthError, match="timed out"):
                request_device_code(API_URL)

    def test_connection_error_raises_auth_error(self):
        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(
                side_effect=httpx.ConnectError("connection refused")
            )

            with pytest.raises(AuthError, match="Failed to connect"):
                request_device_code(API_URL)


# ---------------------------------------------------------------------------
# poll_device_token
# ---------------------------------------------------------------------------


class TestPollDeviceToken:
    @patch("ee_metadata.auth.time.sleep")
    def test_success_after_pending(self, mock_sleep):
        """Token returned after two pending responses."""
        pending = _make_response(400, {"error": "authorization_pending"})
        success = _make_response(
            200, {"token": "jwt-token-123", "refresh_token": "rt-123"}
        )

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(
                side_effect=[pending, pending, success]
            )

            result = poll_device_token(
                "dev-code", API_URL, interval=5, expires_in=600
            )

        assert isinstance(result, ExchangeResult)
        assert result.token == "jwt-token-123"
        assert result.refresh_token == "rt-123"
        assert mock_sleep.call_count == 3
        # All sleep calls should use the same interval (no slow_down)
        mock_sleep.assert_called_with(5)

    @patch("ee_metadata.auth.time.sleep")
    def test_slow_down_increases_interval(self, mock_sleep):
        """Interval increases by 5 on slow_down per RFC 8628."""
        slow_down = _make_response(400, {"error": "slow_down"})
        success = _make_response(
            200, {"token": "jwt-token-456", "refresh_token": "rt-456"}
        )

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(
                side_effect=[slow_down, success]
            )

            result = poll_device_token(
                "dev-code", API_URL, interval=5, expires_in=600
            )

        assert result.token == "jwt-token-456"
        # First call sleeps 5, then slow_down bumps to 10
        assert mock_sleep.call_args_list[0][0] == (5,)
        assert mock_sleep.call_args_list[1][0] == (10,)

    @patch("ee_metadata.auth.time.sleep")
    def test_expired_token_raises_auth_error(self, mock_sleep):
        """AuthError raised when server returns expired_token."""
        expired = _make_response(400, {"error": "expired_token"})

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(return_value=expired)

            with pytest.raises(AuthError, match="expired"):
                poll_device_token("dev-code", API_URL, interval=5, expires_in=600)

    @patch("ee_metadata.auth.time.sleep")
    def test_access_denied_raises_auth_error(self, mock_sleep):
        """AuthError raised when user denies authorization."""
        denied = _make_response(400, {"error": "access_denied"})

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(return_value=denied)

            with pytest.raises(AuthError, match="denied"):
                poll_device_token("dev-code", API_URL, interval=5, expires_in=600)

    @patch("ee_metadata.auth.time.monotonic")
    @patch("ee_metadata.auth.time.sleep")
    def test_timeout_raises_auth_error(self, mock_sleep, mock_monotonic):
        """AuthError raised when expires_in time elapses."""
        # Simulate: start at 0, after first sleep we're at 700 (past 600 expiry)
        mock_monotonic.side_effect = [0, 700]
        pending = _make_response(400, {"error": "authorization_pending"})

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(return_value=pending)

            with pytest.raises(AuthError, match="expired"):
                poll_device_token("dev-code", API_URL, interval=5, expires_in=600)

    @patch("ee_metadata.auth.time.sleep")
    def test_network_error_raises_auth_error(self, mock_sleep):
        """AuthError raised on network failure during polling."""
        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(
                side_effect=httpx.ConnectError("connection refused")
            )

            with pytest.raises(AuthError, match="Failed to connect"):
                poll_device_token("dev-code", API_URL, interval=5, expires_in=600)


# ---------------------------------------------------------------------------
# is_token_expiring_soon
# ---------------------------------------------------------------------------


class TestIsTokenExpiringSoon:
    def test_not_expiring(self):
        """Token with far-future exp is not expiring soon."""
        import base64
        import json
        import time

        payload = {"exp": int(time.time()) + 3600}  # 1 hour from now
        payload_b64 = (
            base64.urlsafe_b64encode(json.dumps(payload).encode())
            .decode()
            .rstrip("=")
        )
        token = f"header.{payload_b64}.signature"

        assert is_token_expiring_soon(token) is False

    def test_expiring_soon(self):
        """Token expiring within threshold should return True."""
        import base64
        import json
        import time

        payload = {"exp": int(time.time()) + 60}  # 1 minute from now
        payload_b64 = (
            base64.urlsafe_b64encode(json.dumps(payload).encode())
            .decode()
            .rstrip("=")
        )
        token = f"header.{payload_b64}.signature"

        assert is_token_expiring_soon(token, threshold=300) is True

    def test_already_expired(self):
        """Already expired token should return True."""
        import base64
        import json
        import time

        payload = {"exp": int(time.time()) - 100}
        payload_b64 = (
            base64.urlsafe_b64encode(json.dumps(payload).encode())
            .decode()
            .rstrip("=")
        )
        token = f"header.{payload_b64}.signature"

        assert is_token_expiring_soon(token) is True

    def test_no_exp_claim(self):
        """Token without exp claim is not considered expiring."""
        import base64
        import json

        payload = {"sub": "user-123"}
        payload_b64 = (
            base64.urlsafe_b64encode(json.dumps(payload).encode())
            .decode()
            .rstrip("=")
        )
        token = f"header.{payload_b64}.signature"

        assert is_token_expiring_soon(token) is False

    def test_invalid_token_returns_true(self):
        """Unparseable token returns True (safe default)."""
        assert is_token_expiring_soon("not-a-jwt") is True


# ---------------------------------------------------------------------------
# refresh_access_token
# ---------------------------------------------------------------------------


class TestRefreshAccessToken:
    def test_success(self):
        """Returns new tokens on 200."""
        mock_response = _make_response(
            200, {"token": "new-jwt", "refresh_token": "new-rt"}
        )

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(return_value=mock_response)

            new_token, new_refresh = refresh_access_token("old-rt", API_URL)

        assert new_token == "new-jwt"
        assert new_refresh == "new-rt"
        MockClient.return_value.post.assert_called_once_with(
            f"{API_URL}/api/cli/refresh",
            json={"refresh_token": "old-rt"},
        )

    def test_401_raises_token_expired(self):
        """401 from server raises TokenExpiredError."""
        mock_response = _make_response(401, {"error": "invalid_grant"})

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(return_value=mock_response)

            with pytest.raises(TokenExpiredError, match="Refresh token expired"):
                refresh_access_token("old-rt", API_URL)

    def test_server_error_raises_auth_error(self):
        """Non-401 error raises AuthError."""
        mock_response = _make_response(500, {"error": "internal"})

        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(return_value=mock_response)

            with pytest.raises(AuthError, match="Token refresh failed"):
                refresh_access_token("old-rt", API_URL)

    def test_timeout_raises_auth_error(self):
        with patch("ee_metadata.auth.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = lambda s: s
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.post = MagicMock(
                side_effect=httpx.TimeoutException("timed out")
            )

            with pytest.raises(AuthError, match="timed out"):
                refresh_access_token("old-rt", API_URL)
