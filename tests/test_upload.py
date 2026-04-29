"""Tests for the upload module."""

from __future__ import annotations

import base64 as _b64
import hashlib
import hmac as _hmac
import os as _os
import tempfile
from pathlib import Path
from threading import Event
from unittest.mock import MagicMock, patch

import httpx
import pytest

from ee_metadata.auth import AuthError
from ee_metadata.upload import (
    AllowedFile,
    ChallengeRange,
    ChallengeResult,
    ClaimChecksum,
    ProjectUploadInfo,
    ResumableSession,
    SignedUrlResponse,
    TokenExpiredUploadError,
    UploadError,
    VerifyResult,
    _b64url_decode,
    _compute_challenge_mac,
    _compute_file_sha256,
    _query_upload_offset,
    _resumable_upload_with_hash,
    _retry_transient,
    _streaming_upload_with_hash,
    claim_by_checksum,
    get_allowed_filenames,
    get_resumable_session,
    match_local_files,
    submit_checksum_challenge,
    upload_file,
    verify_upload,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

API_URL = "https://example.com"
TOKEN = "test-token"
PROJECT_ID = "proj-123"


def _make_allowed(
    name: str,
    sample_id: str = "",
    uploaded: bool = False,
    normalized: str | None = None,
    note_type: str | None = None,
) -> AllowedFile:
    return AllowedFile(
        normalized_name=normalized or name,
        file_name=name,
        sample_id=sample_id or name,
        uploaded=uploaded,
        md5_checksum=None,
        note_type=note_type,
    )


def _tmp_file(content: bytes = b"test data", suffix: str = ".fastq.gz") -> Path:
    """Create a temporary file with given content and return its path."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as f:
        f.write(content)
    return Path(f.name)


# ---------------------------------------------------------------------------
# match_local_files tests
# ---------------------------------------------------------------------------


class TestMatchLocalFiles:
    def test_exact_match(self):
        local = [Path("sample1.fastq.gz"), Path("sample2.fastq.gz")]
        allowed = [
            _make_allowed("sample1.fastq.gz", "s1"),
            _make_allowed("sample2.fastq.gz", "s2"),
        ]

        result = match_local_files(local, allowed)

        assert len(result.matched) == 2
        assert len(result.unmatched_local) == 0
        assert len(result.unmatched_server) == 0

    def test_normalized_match(self):
        local = [Path("Sample_1.fastq.gz")]
        allowed = [
            AllowedFile(
                normalized_name="Sample_1.fastq.gz",
                file_name="sample-1.fastq.gz",
                sample_id="s1",
                uploaded=False,
                md5_checksum=None,
            )
        ]

        result = match_local_files(local, allowed)

        # Matches by normalized_name
        assert len(result.matched) == 1

    def test_unmatched_local(self):
        local = [Path("unknown.fastq.gz")]
        allowed = [_make_allowed("sample1.fastq.gz", "s1")]

        result = match_local_files(local, allowed)

        assert len(result.matched) == 0
        assert len(result.unmatched_local) == 1
        assert result.unmatched_local[0] == Path("unknown.fastq.gz")

    def test_unmatched_server(self):
        local = [Path("sample1.fastq.gz")]
        allowed = [
            _make_allowed("sample1.fastq.gz", "s1"),
            _make_allowed("sample2.fastq.gz", "s2"),
        ]

        result = match_local_files(local, allowed)

        assert len(result.matched) == 1
        assert len(result.unmatched_server) == 1
        assert result.unmatched_server[0].file_name == "sample2.fastq.gz"

    def test_already_uploaded(self):
        local = [Path("sample1.fastq.gz")]
        allowed = [_make_allowed("sample1.fastq.gz", "s1", uploaded=True)]

        result = match_local_files(local, allowed)

        assert len(result.matched) == 0
        assert len(result.already_uploaded) == 1

    def test_mixed_results(self):
        local = [
            Path("ready.fastq.gz"),
            Path("done.fastq.gz"),
            Path("extra.fastq.gz"),
        ]
        allowed = [
            _make_allowed("ready.fastq.gz", "s1"),
            _make_allowed("done.fastq.gz", "s2", uploaded=True),
            _make_allowed("missing.fastq.gz", "s3"),
        ]

        result = match_local_files(local, allowed)

        assert len(result.matched) == 1
        assert len(result.already_uploaded) == 1
        assert len(result.unmatched_local) == 1
        assert len(result.unmatched_server) == 1


# ---------------------------------------------------------------------------
# get_allowed_filenames tests
# ---------------------------------------------------------------------------


def _mock_response(status_code: int, json_data: dict | None = None, text: str = ""):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = {"content-type": "application/json"}
    if json_data is not None:
        resp.json.return_value = json_data
    return resp


class TestGetAllowedFilenames:
    @patch("ee_metadata.upload.httpx.Client")
    def test_success(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_client.get.return_value = _mock_response(
            200,
            {
                "allowedFilenames": [
                    {
                        "normalizedName": "s1.fastq.gz",
                        "fileName": "s1.fastq.gz",
                        "sampleId": "abc",
                        "uploaded": False,
                        "md5CheckSum": None,
                    }
                ],
                "projectMetadataId": "pm-1",
            },
        )

        result = get_allowed_filenames(PROJECT_ID, TOKEN, API_URL)

        assert isinstance(result, ProjectUploadInfo)
        assert len(result.allowed_files) == 1
        assert result.allowed_files[0].sample_id == "abc"
        assert result.project_metadata_id == "pm-1"

    @patch("ee_metadata.upload.httpx.Client")
    def test_401_raises_upload_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(401, text="Unauthorized")

        with pytest.raises(UploadError, match="Authentication failed"):
            get_allowed_filenames(PROJECT_ID, TOKEN, API_URL)

    @patch("ee_metadata.upload.httpx.Client")
    def test_403_raises_upload_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(403, text="Forbidden")

        with pytest.raises(UploadError, match="permission"):
            get_allowed_filenames(PROJECT_ID, TOKEN, API_URL)

    @patch("ee_metadata.upload.httpx.Client")
    def test_404_raises_upload_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(404, text="Not found")

        with pytest.raises(UploadError, match="not found"):
            get_allowed_filenames(PROJECT_ID, TOKEN, API_URL)

    @patch("ee_metadata.upload.httpx.Client")
    def test_timeout_raises_auth_error(self, mock_client_cls):
        mock_client_cls.return_value.__enter__ = MagicMock(
            side_effect=httpx.TimeoutException("timeout")
        )
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(AuthError, match="timed out"):
            get_allowed_filenames(PROJECT_ID, TOKEN, API_URL)


# ---------------------------------------------------------------------------
# _streaming_upload_with_hash tests
# ---------------------------------------------------------------------------


class TestStreamingUpload:
    @staticmethod
    def _make_put_that_consumes_content(status_code=200):
        """Return a mock put() that consumes the content generator."""

        def _put(url, *, content=None, headers=None):
            # Consume the generator so the hasher processes all chunks
            if content is not None:
                for _ in content:
                    pass
            return _mock_response(status_code)

        return _put

    @patch("ee_metadata.upload.httpx.Client")
    def test_upload_computes_correct_hash_and_size(self, mock_client_cls):
        content = b"hello world fastq data" * 100
        filepath = _tmp_file(content)
        expected_hash = hashlib.sha256(content).hexdigest()

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.put.side_effect = self._make_put_that_consumes_content()

        sha, md5, size = _streaming_upload_with_hash(
            filepath, "https://gcs.example.com/upload"
        )

        assert sha == expected_hash
        assert md5 == hashlib.md5(content).hexdigest()
        assert size == len(content)

        filepath.unlink()

    @patch("ee_metadata.upload.httpx.Client")
    def test_upload_calls_progress_callback(self, mock_client_cls):
        content = b"x" * 1024
        filepath = _tmp_file(content)

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.put.side_effect = self._make_put_that_consumes_content()

        callback = MagicMock()
        _streaming_upload_with_hash(
            filepath, "https://gcs.example.com/upload", callback
        )

        assert callback.called
        total_reported = sum(call.args[0] for call in callback.call_args_list)
        assert total_reported == len(content)

        filepath.unlink()

    @patch("ee_metadata.upload.httpx.Client")
    def test_upload_failure_raises_error(self, mock_client_cls):
        filepath = _tmp_file(b"data")

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.put.side_effect = self._make_put_that_consumes_content(403)

        with pytest.raises(UploadError, match="GCS upload failed"):
            _streaming_upload_with_hash(filepath, "https://gcs.example.com/upload")

        filepath.unlink()


# ---------------------------------------------------------------------------
# upload_file integration test
# ---------------------------------------------------------------------------


class TestUploadFile:
    @patch("ee_metadata.upload.verify_upload")
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._resumable_upload_with_hash")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_success_flow_resumable(
        self,
        mock_config_dir,
        mock_get_session,
        mock_stream,
        mock_complete,
        mock_verify,
        tmp_path,
    ):
        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")

        mock_get_session.return_value = ResumableSession(
            session_uri="https://storage.googleapis.com/upload/session/abc",
            sample_id="s1",
            file_id="f1",
        )
        mock_stream.return_value = ("abc123hash", "def456md5", 4)
        mock_verify.return_value = VerifyResult(
            ok=True, remote_md5="3vu+", remote_size=4, local_md5="3vu+", local_size=4
        )

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
        )

        assert result.success
        assert result.checksum == "abc123hash"
        assert result.filesize == 4
        mock_complete.assert_called_once()

        filepath.unlink()

    @patch("ee_metadata.upload.verify_upload")
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._streaming_upload_with_hash")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.upload.get_signed_url")
    @patch("ee_metadata.resume_store._config_dir")
    def test_fallback_to_signed_url(
        self,
        mock_config_dir,
        mock_get_url,
        mock_get_session,
        mock_stream,
        mock_complete,
        mock_verify,
        tmp_path,
    ):
        """Falls back to signed URL when resumable endpoint returns 404."""
        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")

        mock_get_session.side_effect = UploadError(
            "Resumable upload not available (404): Not found"
        )
        mock_get_url.return_value = SignedUrlResponse(
            signed_url="https://gcs.example.com/upload",
            sample_id="s1",
            file_id="f1",
        )
        mock_stream.return_value = ("abc123hash", "def456md5", 4)
        mock_verify.return_value = VerifyResult(
            ok=True, remote_md5="3vu+", remote_size=4, local_md5="3vu+", local_size=4
        )

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
        )

        assert result.success
        mock_get_url.assert_called_once()

        filepath.unlink()

    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_failure_returns_error_result(
        self, mock_config_dir, mock_get_session, tmp_path
    ):
        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")

        mock_get_session.side_effect = UploadError("no permission")

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
        )

        assert not result.success
        assert "no permission" in result.error

        filepath.unlink()


# ---------------------------------------------------------------------------
# REUPLOAD matching tests
# ---------------------------------------------------------------------------


class TestReuploadMatching:
    def test_reupload_files_in_needs_reupload(self):
        """File with uploaded=True and note_type='REUPLOAD' goes to needs_reupload."""
        local = [Path("sample1.fastq.gz")]
        allowed = [
            _make_allowed("sample1.fastq.gz", "s1", uploaded=True, note_type="REUPLOAD")
        ]

        result = match_local_files(local, allowed)

        assert len(result.needs_reupload) == 1
        assert len(result.already_uploaded) == 0
        assert len(result.matched) == 0

    def test_uploaded_without_reupload_stays_in_already_uploaded(self):
        """File with uploaded=True and no note_type stays in already_uploaded."""
        local = [Path("sample1.fastq.gz")]
        allowed = [_make_allowed("sample1.fastq.gz", "s1", uploaded=True)]

        result = match_local_files(local, allowed)

        assert len(result.needs_reupload) == 0
        assert len(result.already_uploaded) == 1

    def test_uploaded_with_warning_stays_in_already_uploaded(self):
        """File with uploaded=True and note_type='WARNING' stays in already_uploaded."""
        local = [Path("sample1.fastq.gz")]
        allowed = [
            _make_allowed("sample1.fastq.gz", "s1", uploaded=True, note_type="WARNING")
        ]

        result = match_local_files(local, allowed)

        assert len(result.needs_reupload) == 0
        assert len(result.already_uploaded) == 1


# ---------------------------------------------------------------------------
# Circuit breaker & cancel_event tests
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    def test_cancel_event_skips_upload(self):
        """Pre-set cancel event causes upload_file to return skipped=True immediately."""
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")
        cancel = Event()
        cancel.set()

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
            cancel_event=cancel,
        )

        assert not result.success
        assert result.skipped

        filepath.unlink()

    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_401_sets_cancel_event(self, mock_config_dir, mock_get_session, tmp_path):
        """TokenExpiredUploadError from get_resumable_session sets the cancel_event."""
        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")
        cancel = Event()

        mock_get_session.side_effect = TokenExpiredUploadError("Token expired")

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
            cancel_event=cancel,
        )

        assert not result.success
        assert not result.skipped  # First failure is not "skipped", it's the trigger
        assert cancel.is_set()

        filepath.unlink()

    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._resumable_upload_with_hash")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_cancel_after_gcs_put_skips_complete(
        self, mock_config_dir, mock_get_session, mock_stream, mock_complete, tmp_path
    ):
        """If cancel_event is set after GCS PUT, complete_upload is skipped."""
        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")
        cancel = Event()

        mock_get_session.return_value = ResumableSession(
            session_uri="https://storage.googleapis.com/upload/session/abc",
            sample_id="s1",
            file_id="f1",
        )

        def _set_cancel_and_return(*args, **kwargs):
            cancel.set()
            return ("abc123hash", "def456md5", 4)

        mock_stream.side_effect = _set_cancel_and_return

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
            cancel_event=cancel,
        )

        assert not result.success
        assert result.skipped
        mock_complete.assert_not_called()

        filepath.unlink()

    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._resumable_upload_with_hash")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_cancel_after_gcs_put_preserves_resume_state(
        self, mock_config_dir, mock_get_session, mock_stream, mock_complete, tmp_path
    ):
        """Resume state is NOT cleared when cancel_event fires after GCS upload."""
        from ee_metadata.resume_store import _state_path

        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")
        cancel = Event()

        mock_get_session.return_value = ResumableSession(
            session_uri="https://storage.googleapis.com/upload/session/abc",
            sample_id="s1",
            file_id="f1",
        )

        def _set_cancel_and_return(*args, **kwargs):
            cancel.set()
            return ("abc123hash", "def456md5", 4)

        mock_stream.side_effect = _set_cancel_and_return

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
            cancel_event=cancel,
        )

        assert result.skipped
        # Resume state should still exist so next run can skip re-upload
        state_file = _state_path(PROJECT_ID, filepath.name)
        assert state_file.exists(), "Resume state should be preserved after cancel"

        filepath.unlink()


# ---------------------------------------------------------------------------
# Retry helper tests
# ---------------------------------------------------------------------------


class TestRetryTransient:
    @patch("ee_metadata.upload.time.sleep")
    def test_retries_transient_then_succeeds(self, mock_sleep):
        """First call raises transient error, second succeeds."""
        call_count = 0

        def _flaky(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise UploadError("Server returned status 500: Internal Server Error")
            return "ok"

        result = _retry_transient(_flaky, max_retries=3)

        assert result == "ok"
        assert call_count == 2
        mock_sleep.assert_called_once()

    @patch("ee_metadata.upload.time.sleep")
    def test_no_retry_on_permission_error(self, mock_sleep):
        """Permission errors are not retried."""
        call_count = 0

        def _perm_error(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise UploadError("You don't have permission to upload to this project.")

        with pytest.raises(UploadError, match="permission"):
            _retry_transient(_perm_error, max_retries=3)

        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("ee_metadata.upload.time.sleep")
    def test_no_retry_on_not_found_error(self, mock_sleep):
        """Not found errors are not retried."""
        call_count = 0

        def _not_found(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise UploadError("Project 'xyz' not found.")

        with pytest.raises(UploadError, match="not found"):
            _retry_transient(_not_found, max_retries=3)

        assert call_count == 1
        mock_sleep.assert_not_called()

    def test_no_retry_on_token_expired(self):
        """TokenExpiredUploadError is never retried."""
        call_count = 0

        def _expired(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise TokenExpiredUploadError("Token expired")

        with pytest.raises(TokenExpiredUploadError):
            _retry_transient(_expired, max_retries=3)

        assert call_count == 1

    @patch("ee_metadata.upload.time.sleep")
    def test_cancel_event_aborts_retry(self, mock_sleep):
        """If cancel_event is set, retry bails immediately."""
        cancel = Event()
        cancel.set()

        def _should_not_run(*args, **kwargs):
            raise AssertionError("Should not be called")

        with pytest.raises(TokenExpiredUploadError, match="cancelled"):
            _retry_transient(_should_not_run, cancel_event=cancel)

    @patch("ee_metadata.upload.time.sleep")
    def test_retries_auth_error(self, mock_sleep):
        """AuthError (connection errors) are retried."""
        call_count = 0

        def _conn_error(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise AuthError("Failed to connect")
            return "connected"

        result = _retry_transient(_conn_error, max_retries=3)

        assert result == "connected"
        assert call_count == 3


# ---------------------------------------------------------------------------
# noteType parsing test
# ---------------------------------------------------------------------------


class TestParseNoteType:
    @patch("ee_metadata.upload.httpx.Client")
    def test_parse_note_type(self, mock_client_cls):
        """Server response with noteType='REUPLOAD' sets AllowedFile.note_type."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_client.get.return_value = _mock_response(
            200,
            {
                "allowedFilenames": [
                    {
                        "normalizedName": "s1.fastq.gz",
                        "fileName": "s1.fastq.gz",
                        "sampleId": "abc",
                        "uploaded": True,
                        "md5CheckSum": "abc123",
                        "noteType": "REUPLOAD",
                    }
                ],
                "projectMetadataId": "pm-1",
            },
        )

        result = get_allowed_filenames(PROJECT_ID, TOKEN, API_URL)

        assert result.allowed_files[0].note_type == "REUPLOAD"
        assert result.allowed_files[0].uploaded is True

    @patch("ee_metadata.upload.httpx.Client")
    def test_parse_note_type_null(self, mock_client_cls):
        """Server response without noteType sets AllowedFile.note_type to None."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_client.get.return_value = _mock_response(
            200,
            {
                "allowedFilenames": [
                    {
                        "normalizedName": "s1.fastq.gz",
                        "fileName": "s1.fastq.gz",
                        "sampleId": "abc",
                        "uploaded": False,
                        "md5CheckSum": None,
                    }
                ],
                "projectMetadataId": "pm-1",
            },
        )

        result = get_allowed_filenames(PROJECT_ID, TOKEN, API_URL)

        assert result.allowed_files[0].note_type is None


# ---------------------------------------------------------------------------
# Resumable session request tests
# ---------------------------------------------------------------------------


class TestGetResumableSession:
    @patch("ee_metadata.upload.httpx.Client")
    def test_success(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_client.post.return_value = _mock_response(
            200,
            {
                "sessionUri": "https://storage.googleapis.com/upload/session/abc",
                "sampleId": "s1",
                "fileId": "f1",
            },
        )

        result = get_resumable_session(PROJECT_ID, "sample.fastq.gz", TOKEN, API_URL)

        assert isinstance(result, ResumableSession)
        assert result.session_uri == "https://storage.googleapis.com/upload/session/abc"
        assert result.sample_id == "s1"
        assert result.file_id == "f1"

    @patch("ee_metadata.upload.httpx.Client")
    def test_401_raises_token_expired(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(401, text="Unauthorized")

        with pytest.raises(TokenExpiredUploadError, match="Token expired"):
            get_resumable_session(PROJECT_ID, "sample.fastq.gz", TOKEN, API_URL)

    @patch("ee_metadata.upload.httpx.Client")
    def test_404_raises_upload_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(404, text="Not found")

        with pytest.raises(UploadError, match="not available"):
            get_resumable_session(PROJECT_ID, "sample.fastq.gz", TOKEN, API_URL)


# ---------------------------------------------------------------------------
# Query upload offset tests
# ---------------------------------------------------------------------------


class TestQueryUploadOffset:
    @patch("ee_metadata.upload.httpx.Client")
    def test_308_returns_offset(self, mock_client_cls):
        """308 with Range header returns confirmed byte offset."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 308
        resp.headers = {"Range": "bytes=0-524287"}
        mock_client.put.return_value = resp

        offset = _query_upload_offset("https://session-uri", 1048576)

        assert offset == 524288  # 524287 + 1

    @patch("ee_metadata.upload.httpx.Client")
    def test_200_means_complete(self, mock_client_cls):
        """200 response means upload is already complete."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        mock_client.put.return_value = resp

        offset = _query_upload_offset("https://session-uri", 1048576)

        assert offset == 1048576

    @patch("ee_metadata.upload.httpx.Client")
    def test_404_returns_zero(self, mock_client_cls):
        """404 response means session expired → return 0."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 404
        mock_client.put.return_value = resp

        offset = _query_upload_offset("https://session-uri", 1048576)

        assert offset == 0

    @patch("ee_metadata.upload.httpx.Client")
    def test_timeout_returns_zero(self, mock_client_cls):
        """Timeout returns 0."""
        mock_client_cls.return_value.__enter__ = MagicMock(
            side_effect=httpx.TimeoutException("timeout")
        )
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        offset = _query_upload_offset("https://session-uri", 1048576)

        assert offset == 0


# ---------------------------------------------------------------------------
# Resumable upload with hash tests
# ---------------------------------------------------------------------------


class TestResumableUploadWithHash:
    @patch("ee_metadata.resume_store._config_dir")
    @patch("ee_metadata.upload.httpx.Client")
    def test_full_upload(self, mock_client_cls, mock_config_dir, tmp_path):
        """Full upload from offset 0 computes correct hashes."""
        mock_config_dir.return_value = tmp_path / "config"
        content = b"hello world fastq data" * 100
        filepath = _tmp_file(content)
        expected_sha = hashlib.sha256(content).hexdigest()
        expected_md5 = hashlib.md5(content).hexdigest()

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        # All chunks return 308 except we'll just have them all 308
        # (the function checks for 200/201 on last chunk too)
        resp_308 = MagicMock(spec=httpx.Response)
        resp_308.status_code = 308
        resp_200 = MagicMock(spec=httpx.Response)
        resp_200.status_code = 200
        mock_client.put.return_value = resp_200

        sha, md5, size = _resumable_upload_with_hash(
            filepath,
            "https://session-uri",
            PROJECT_ID,
            len(content),
            resume_offset=0,
        )

        assert sha == expected_sha
        assert md5 == expected_md5
        assert size == len(content)

        filepath.unlink()

    @patch("ee_metadata.resume_store._config_dir")
    @patch("ee_metadata.upload.httpx.Client")
    def test_resumed_upload_correct_hashes(
        self, mock_client_cls, mock_config_dir, tmp_path
    ):
        """Resuming from offset still produces correct full-file hashes."""
        mock_config_dir.return_value = tmp_path / "config"
        content = b"A" * 1024 + b"B" * 1024  # 2KB file
        filepath = _tmp_file(content)
        expected_sha = hashlib.sha256(content).hexdigest()
        expected_md5 = hashlib.md5(content).hexdigest()

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        resp_200 = MagicMock(spec=httpx.Response)
        resp_200.status_code = 200
        mock_client.put.return_value = resp_200

        # Resume from offset 1024 (first 1KB already uploaded)
        sha, md5, size = _resumable_upload_with_hash(
            filepath,
            "https://session-uri",
            PROJECT_ID,
            len(content),
            resume_offset=1024,
        )

        # Hashes should be over ENTIRE file, not just the resumed portion
        assert sha == expected_sha
        assert md5 == expected_md5
        assert size == len(content)

        filepath.unlink()

    @patch("ee_metadata.resume_store._config_dir")
    @patch("ee_metadata.upload.httpx.Client")
    def test_calls_progress_callback(self, mock_client_cls, mock_config_dir, tmp_path):
        """Progress callback receives resume offset + chunk bytes."""
        mock_config_dir.return_value = tmp_path / "config"
        content = b"x" * 1024
        filepath = _tmp_file(content)

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        resp_200 = MagicMock(spec=httpx.Response)
        resp_200.status_code = 200
        mock_client.put.return_value = resp_200

        callback = MagicMock()
        _resumable_upload_with_hash(
            filepath,
            "https://session-uri",
            PROJECT_ID,
            len(content),
            resume_offset=0,
            progress_callback=callback,
        )

        assert callback.called
        total_reported = sum(call.args[0] for call in callback.call_args_list)
        assert total_reported == len(content)

        filepath.unlink()


# ---------------------------------------------------------------------------
# Verify upload tests
# ---------------------------------------------------------------------------


class TestVerifyUpload:
    @patch("ee_metadata.upload.httpx.Client")
    def test_success_match(self, mock_client_cls):
        """Matching md5 and size returns ok=True."""
        import base64

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        # GCS returns MD5 as base64
        local_md5_hex = hashlib.md5(b"test data").hexdigest()
        remote_md5_b64 = base64.b64encode(bytes.fromhex(local_md5_hex)).decode()

        mock_client.post.return_value = _mock_response(
            200,
            {"md5Hash": remote_md5_b64, "size": 9},
        )

        result = verify_upload(
            PROJECT_ID, "sample.fastq.gz", local_md5_hex, 9, TOKEN, API_URL
        )

        assert result.ok is True
        assert result.remote_md5 == remote_md5_b64

    @patch("ee_metadata.upload.httpx.Client")
    def test_mismatch(self, mock_client_cls):
        """Different md5 returns ok=False."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_client.post.return_value = _mock_response(
            200,
            {"md5Hash": "AAAA", "size": 9},
        )

        result = verify_upload(
            PROJECT_ID, "sample.fastq.gz", "abcdef1234567890", 9, TOKEN, API_URL
        )

        assert result.ok is False

    @patch("ee_metadata.upload.httpx.Client")
    def test_404_raises_upload_error(self, mock_client_cls):
        """404 response raises UploadError."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(404, text="Not found")

        with pytest.raises(UploadError, match="not found"):
            verify_upload(PROJECT_ID, "sample.fastq.gz", "abc", 9, TOKEN, API_URL)


# ---------------------------------------------------------------------------
# FastQ-vault PoW dedup tests
# ---------------------------------------------------------------------------


def _make_nonce_b64url(raw: bytes = b"\x01" * 32) -> str:
    """Return a nonce encoded the same way the server emits it (base64url no pad)."""
    return _b64.urlsafe_b64encode(raw).rstrip(b"=").decode()


class TestClaimByChecksum:
    @patch("ee_metadata.upload.httpx.Client")
    def test_returns_upload_action(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(
            200, {"action": "upload", "reason": "not_in_vault"}
        )

        result = claim_by_checksum(
            project_metadata_id="pm-1",
            sample_id="s1",
            file_name="sample.fastq.gz",
            checksum="a" * 64,
            filesize=1024,
            token=TOKEN,
            api_url=API_URL,
        )

        assert isinstance(result, ClaimChecksum)
        assert result.action == "upload"
        assert result.reason == "not_in_vault"
        assert result.ranges is None

    @patch("ee_metadata.upload.httpx.Client")
    def test_returns_challenge(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(
            200,
            {
                "action": "challenge",
                "token": "jwt.token.here",
                "nonce": _make_nonce_b64url(),
                "ranges": [
                    {"offset": 0, "length": 8192},
                    {"offset": 100000, "length": 131072},
                ],
                "expiresAt": "2026-04-29T12:00:00.000Z",
            },
        )

        result = claim_by_checksum(
            project_metadata_id="pm-1",
            sample_id="s1",
            file_name="sample.fastq.gz",
            checksum="b" * 64,
            filesize=1_000_000,
            token=TOKEN,
            api_url=API_URL,
        )

        assert result.action == "challenge"
        assert result.token == "jwt.token.here"
        assert result.ranges is not None
        assert len(result.ranges) == 2
        assert isinstance(result.ranges[0], ChallengeRange)
        assert result.ranges[0].offset == 0
        assert result.ranges[0].length == 8192
        assert result.ranges[1].offset == 100000

    @patch("ee_metadata.upload.httpx.Client")
    def test_401_raises_token_expired(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(401, text="Unauthorized")

        with pytest.raises(TokenExpiredUploadError):
            claim_by_checksum(
                project_metadata_id="pm-1",
                sample_id="s1",
                file_name="x.fastq.gz",
                checksum="c" * 64,
                filesize=1024,
                token=TOKEN,
                api_url=API_URL,
            )

    @patch("ee_metadata.upload.httpx.Client")
    def test_500_raises_upload_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(500, text="boom")

        with pytest.raises(UploadError, match="claim-by-checksum failed"):
            claim_by_checksum(
                project_metadata_id="pm-1",
                sample_id="s1",
                file_name="x.fastq.gz",
                checksum="c" * 64,
                filesize=1024,
                token=TOKEN,
                api_url=API_URL,
            )


class TestSubmitChecksumChallenge:
    @patch("ee_metadata.upload.httpx.Client")
    def test_linked(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(
            200,
            {
                "status": "linked",
                "fastqFileId": "ff-1",
                "verifyTriggered": True,
            },
        )

        result = submit_checksum_challenge(
            challenge_token="jwt", mac="d" * 64, token=TOKEN, api_url=API_URL
        )

        assert isinstance(result, ChallengeResult)
        assert result.status == "linked"
        assert result.fastq_file_id == "ff-1"
        assert result.verify_triggered is True

    @patch("ee_metadata.upload.httpx.Client")
    def test_403_raises_upload_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(
            403, text="Challenge already used."
        )

        with pytest.raises(UploadError, match="denied"):
            submit_checksum_challenge(
                challenge_token="jwt", mac="d" * 64, token=TOKEN, api_url=API_URL
            )

    @patch("ee_metadata.upload.httpx.Client")
    def test_401_raises_token_expired(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = _mock_response(401, text="Unauthorized")

        with pytest.raises(TokenExpiredUploadError):
            submit_checksum_challenge(
                challenge_token="jwt", mac="d" * 64, token=TOKEN, api_url=API_URL
            )


class TestComputeChallengeMac:
    def test_matches_independent_computation(self, tmp_path):
        """Hex digest matches a stdlib HMAC over the concatenated ranges."""
        # Random-ish 1 MB blob.
        blob = _os.urandom(1_000_000)
        filepath = tmp_path / "blob.bin"
        filepath.write_bytes(blob)

        nonce_raw = b"\x05" * 32
        nonce_b64 = _make_nonce_b64url(nonce_raw)
        ranges = [
            ChallengeRange(offset=0, length=8192),
            ChallengeRange(offset=500_000, length=8192),
            ChallengeRange(offset=900_000, length=99_999),
        ]

        # Independent computation, server-style.
        msg = b"".join(blob[r.offset : r.offset + r.length] for r in ranges)
        expected = _hmac.new(nonce_raw, msg, hashlib.sha256).hexdigest()

        actual = _compute_challenge_mac(filepath, nonce_b64, ranges)

        assert actual == expected

    def test_b64url_decode_roundtrip(self):
        """Nonce decoder handles missing padding the way the server emits."""
        for n in range(20, 40):
            raw = b"\xab" * n
            assert _b64url_decode(_make_nonce_b64url(raw)) == raw

    def test_b64url_decode_malformed_raises_uploaderror(self):
        """A malformed nonce surfaces as UploadError so the dedup branch's
        fallback handler catches it instead of crashing the worker.

        Inputs whose length mod 4 == 1 are not legal base64; stdlib
        raises ``binascii.Error`` rather than silently decoding.
        """
        with pytest.raises(UploadError, match="Invalid base64url nonce"):
            _b64url_decode("A")  # length-1 → invalid byte count

    def test_short_read_raises(self, tmp_path):
        """If a range extends beyond EOF, an UploadError is raised."""
        filepath = tmp_path / "tiny.bin"
        filepath.write_bytes(b"x" * 100)

        with pytest.raises(UploadError, match="short read"):
            _compute_challenge_mac(
                filepath,
                _make_nonce_b64url(),
                [ChallengeRange(offset=50, length=200)],
            )


class TestComputeFileSha256:
    def test_matches_hashlib(self, tmp_path):
        content = b"hello world\n" * 1000
        filepath = tmp_path / "f.bin"
        filepath.write_bytes(content)

        assert _compute_file_sha256(filepath) == hashlib.sha256(content).hexdigest()


class TestUploadFileDedup:
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload.submit_checksum_challenge")
    @patch("ee_metadata.upload.claim_by_checksum")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.upload.get_signed_url")
    @patch("ee_metadata.resume_store._config_dir")
    def test_skips_upload_on_dedup(
        self,
        mock_config_dir,
        mock_get_url,
        mock_get_session,
        mock_claim,
        mock_submit,
        mock_complete,
        tmp_path,
    ):
        """When server returns a challenge and we answer it, no upload happens."""
        mock_config_dir.return_value = tmp_path / "config"
        content = b"some fastq bytes" * 100
        filepath = _tmp_file(content)
        af = _make_allowed(filepath.name, "s1")

        mock_claim.return_value = ClaimChecksum(
            action="challenge",
            token="jwt",
            nonce=_make_nonce_b64url(),
            ranges=[ChallengeRange(offset=0, length=16)],
            expires_at="2026-04-29T12:00:00Z",
        )
        mock_submit.return_value = ChallengeResult(
            status="linked", fastq_file_id="ff-1", verify_triggered=True
        )

        progress = MagicMock()
        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
            progress_callback=progress,
        )

        assert result.success
        assert result.deduped is True
        assert result.checksum == hashlib.sha256(content).hexdigest()
        assert result.filesize == len(content)
        # Normal upload path must NOT have run.
        mock_get_session.assert_not_called()
        mock_get_url.assert_not_called()
        mock_complete.assert_not_called()
        # Progress bar advanced by the full filesize.
        progress.assert_called_with(len(content))

        filepath.unlink()

    @patch("ee_metadata.upload.verify_upload")
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._resumable_upload_with_hash")
    @patch("ee_metadata.upload.submit_checksum_challenge")
    @patch("ee_metadata.upload.claim_by_checksum")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_falls_back_when_challenge_denied(
        self,
        mock_config_dir,
        mock_get_session,
        mock_claim,
        mock_submit,
        mock_stream,
        mock_complete,
        mock_verify,
        tmp_path,
    ):
        """403 from submit-challenge → fall back to normal upload."""
        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")

        mock_claim.return_value = ClaimChecksum(
            action="challenge",
            token="jwt",
            nonce=_make_nonce_b64url(),
            ranges=[ChallengeRange(offset=0, length=4)],
        )
        mock_submit.side_effect = UploadError(
            "Vault dedup challenge denied; will retry as a normal upload."
        )
        mock_get_session.return_value = ResumableSession(
            session_uri="https://storage.googleapis.com/upload/session/abc",
            sample_id="s1",
            file_id="f1",
        )
        mock_stream.return_value = ("abc123hash", "def456md5", 4)
        mock_verify.return_value = VerifyResult(
            ok=True, remote_md5="3vu+", remote_size=4, local_md5="3vu+", local_size=4
        )

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
        )

        assert result.success
        assert result.deduped is False
        mock_get_session.assert_called_once()
        mock_complete.assert_called_once()

        filepath.unlink()

    @patch("ee_metadata.upload.verify_upload")
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._resumable_upload_with_hash")
    @patch("ee_metadata.upload.submit_checksum_challenge")
    @patch("ee_metadata.upload.claim_by_checksum")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_falls_back_when_challenge_nonce_malformed(
        self,
        mock_config_dir,
        mock_get_session,
        mock_claim,
        mock_submit,
        mock_stream,
        mock_complete,
        mock_verify,
        tmp_path,
    ):
        """A malformed nonce from the server must not crash the worker.

        Regression for greptile P1: ``base64.urlsafe_b64decode`` raises
        ``binascii.Error`` (not ``UploadError``), which would otherwise
        escape the dedup fallback handler and propagate out through
        ``future.result()`` to abort the whole CLI command.
        """
        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")

        mock_claim.return_value = ClaimChecksum(
            action="challenge",
            token="jwt",
            # 41 chars (length mod 4 == 1) — stdlib raises binascii.Error
            # rather than silently decoding. Stand-in for a corrupted /
            # truncated nonce reaching us from the server.
            nonce="A" * 41,
            ranges=[ChallengeRange(offset=0, length=4)],
        )
        mock_get_session.return_value = ResumableSession(
            session_uri="https://storage.googleapis.com/upload/session/abc",
            sample_id="s1",
            file_id="f1",
        )
        mock_stream.return_value = ("abc123hash", "def456md5", 4)
        mock_verify.return_value = VerifyResult(
            ok=True, remote_md5="3vu+", remote_size=4, local_md5="3vu+", local_size=4
        )

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
        )

        # Did not crash; fell back to the normal upload path.
        assert result.success
        assert result.deduped is False
        mock_submit.assert_not_called()
        mock_get_session.assert_called_once()
        mock_complete.assert_called_once()

        filepath.unlink()

    @patch("ee_metadata.upload.verify_upload")
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._resumable_upload_with_hash")
    @patch("ee_metadata.upload.submit_checksum_challenge")
    @patch("ee_metadata.upload.claim_by_checksum")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_action_upload_uses_normal_path(
        self,
        mock_config_dir,
        mock_get_session,
        mock_claim,
        mock_submit,
        mock_stream,
        mock_complete,
        mock_verify,
        tmp_path,
    ):
        """When server says 'upload' (not in vault), normal flow runs and submit is never called."""
        mock_config_dir.return_value = tmp_path / "config"
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")

        mock_claim.return_value = ClaimChecksum(action="upload", reason="not_in_vault")
        mock_get_session.return_value = ResumableSession(
            session_uri="https://storage.googleapis.com/upload/session/abc",
            sample_id="s1",
            file_id="f1",
        )
        mock_stream.return_value = ("abc123hash", "def456md5", 4)
        mock_verify.return_value = VerifyResult(
            ok=True, remote_md5="3vu+", remote_size=4, local_md5="3vu+", local_size=4
        )

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
        )

        assert result.success
        assert result.deduped is False
        mock_submit.assert_not_called()
        mock_get_session.assert_called_once()

        filepath.unlink()

    @patch("ee_metadata.upload.submit_checksum_challenge")
    @patch("ee_metadata.upload.claim_by_checksum")
    def test_token_expired_during_claim_trips_circuit_breaker(
        self, mock_claim, mock_submit, tmp_path
    ):
        """401 from claim_by_checksum sets cancel_event and returns failure."""
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")
        cancel = Event()

        mock_claim.side_effect = TokenExpiredUploadError("Token expired")

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
            cancel_event=cancel,
        )

        assert not result.success
        assert cancel.is_set()
        mock_submit.assert_not_called()

        filepath.unlink()


# ---------------------------------------------------------------------------
# Precomputed SHA-256 plumbing (greptile P2: avoid double SHA-256 pass)
# ---------------------------------------------------------------------------


class TestPrecomputedSha256:
    @staticmethod
    def _make_put_that_consumes_content(status_code=200):
        def _put(url, *, content=None, headers=None):
            if content is not None:
                for _ in content:
                    pass
            return _mock_response(status_code)

        return _put

    @patch("ee_metadata.upload.httpx.Client")
    def test_streaming_upload_skips_sha256_when_precomputed(self, mock_client_cls):
        """Helper trusts the supplied SHA-256 instead of recomputing.

        Returning the precomputed value rather than the real file SHA
        proves the inner hasher was never fed.
        """
        content = b"hello world fastq data" * 100
        filepath = _tmp_file(content)
        sentinel = "f" * 64  # distinct from the real SHA

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.put.side_effect = self._make_put_that_consumes_content()

        sha, md5, size = _streaming_upload_with_hash(
            filepath,
            "https://gcs.example.com/upload",
            precomputed_sha256=sentinel,
        )

        assert sha == sentinel
        # MD5 must still be the real one — verify_upload depends on it.
        assert md5 == hashlib.md5(content).hexdigest()
        assert size == len(content)

        filepath.unlink()

    @patch("ee_metadata.resume_store._config_dir")
    @patch("ee_metadata.upload.httpx.Client")
    def test_resumable_upload_skips_sha256_when_precomputed(
        self, mock_client_cls, mock_config_dir, tmp_path
    ):
        """Resumable path with a partial-resume offset still trusts the
        precomputed value — the resume-replay must not re-hash bytes."""
        mock_config_dir.return_value = tmp_path / "config"
        content = b"A" * 1024 + b"B" * 1024
        filepath = _tmp_file(content)
        sentinel = "0" * 64

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        resp_200 = MagicMock(spec=httpx.Response)
        resp_200.status_code = 200
        mock_client.put.return_value = resp_200

        sha, md5, size = _resumable_upload_with_hash(
            filepath,
            "https://session-uri",
            PROJECT_ID,
            len(content),
            resume_offset=1024,
            precomputed_sha256=sentinel,
        )

        assert sha == sentinel
        # MD5 must still cover the entire file (replay + remaining bytes).
        assert md5 == hashlib.md5(content).hexdigest()
        assert size == len(content)

        filepath.unlink()

    @patch("ee_metadata.upload.verify_upload")
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._streaming_upload_with_hash")
    @patch("ee_metadata.upload.submit_checksum_challenge")
    @patch("ee_metadata.upload.claim_by_checksum")
    @patch("ee_metadata.upload.get_resumable_session")
    @patch("ee_metadata.resume_store._config_dir")
    def test_upload_file_threads_precomputed_sha_on_action_upload(
        self,
        mock_config_dir,
        mock_get_session,
        mock_claim,
        mock_submit,
        mock_stream,
        mock_complete,
        mock_verify,
        tmp_path,
    ):
        """When the server returns action='upload', the pre-pass SHA-256
        flows down into the upload helper so it skips the redundant pass."""
        mock_config_dir.return_value = tmp_path / "config"
        content = b"some fastq bytes" * 100
        filepath = _tmp_file(content)
        af = _make_allowed(filepath.name, "s1")
        expected_sha = hashlib.sha256(content).hexdigest()

        mock_claim.return_value = ClaimChecksum(
            action="upload", reason="not_in_vault"
        )
        # Force the streaming fallback path (resumable session unavailable).
        mock_get_session.side_effect = UploadError("resumable not available")
        # Avoid actually patching get_signed_url — instead mock the helper
        # the streaming branch ends up calling.
        mock_stream.return_value = (expected_sha, "md5hex", len(content))
        mock_verify.return_value = VerifyResult(
            ok=True,
            remote_md5="x",
            remote_size=len(content),
            local_md5="x",
            local_size=len(content),
        )

        with patch("ee_metadata.upload.get_signed_url") as mock_get_url:
            mock_get_url.return_value = SignedUrlResponse(
                signed_url="https://gcs.example.com/upload",
                sample_id="s1",
                file_id="f1",
            )
            result = upload_file(
                filepath=filepath,
                allowed_file=af,
                project_id=PROJECT_ID,
                project_metadata_id="pm-1",
                token=TOKEN,
                api_url=API_URL,
            )

        assert result.success
        assert result.deduped is False
        mock_submit.assert_not_called()
        # The helper was called with precomputed_sha256 set to the pre-pass value.
        _, kwargs = mock_stream.call_args
        assert kwargs.get("precomputed_sha256") == expected_sha

        filepath.unlink()

    @patch("ee_metadata.upload.verify_upload")
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._resumable_upload_with_hash")
    @patch("ee_metadata.upload.claim_by_checksum")
    @patch("ee_metadata.upload._query_upload_offset")
    @patch("ee_metadata.resume_store._config_dir")
    def test_upload_file_no_precomputed_sha_when_resume_state_exists(
        self,
        mock_config_dir,
        mock_query_offset,
        mock_claim,
        mock_resumable,
        mock_complete,
        mock_verify,
        tmp_path,
    ):
        """When a resume state is loaded, the dedup pre-pass is skipped, so
        the helper must be called with precomputed_sha256=None."""
        import time

        from ee_metadata.resume_store import ResumeState, save_resume_state

        mock_config_dir.return_value = tmp_path / "config"
        content = b"x" * 4096
        filepath = _tmp_file(content)
        af = _make_allowed(filepath.name, "s1")

        save_resume_state(
            ResumeState(
                session_uri="https://storage.googleapis.com/upload/session/abc",
                project_id=PROJECT_ID,
                filename=filepath.name,
                filesize=len(content),
                file_mtime=filepath.stat().st_mtime,
                bytes_uploaded=1024,
                sample_id="s1",
                file_id="f1",
                created_at=time.time(),
            )
        )
        mock_query_offset.return_value = 1024
        mock_resumable.return_value = ("sha", "md5", len(content))
        mock_verify.return_value = VerifyResult(
            ok=True,
            remote_md5="x",
            remote_size=len(content),
            local_md5="x",
            local_size=len(content),
        )

        result = upload_file(
            filepath=filepath,
            allowed_file=af,
            project_id=PROJECT_ID,
            project_metadata_id="pm-1",
            token=TOKEN,
            api_url=API_URL,
        )

        assert result.success
        # Dedup path was skipped entirely.
        mock_claim.assert_not_called()
        # Helper got precomputed_sha256=None — let it compute as today.
        _, kwargs = mock_resumable.call_args
        assert kwargs.get("precomputed_sha256") is None

        filepath.unlink()
