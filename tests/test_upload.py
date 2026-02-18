"""Tests for the upload module."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
from threading import Event
from unittest.mock import MagicMock, patch

import httpx
import pytest

from ee_metadata.auth import AuthError
from ee_metadata.upload import (
    AllowedFile,
    ProjectUploadInfo,
    ResumableSession,
    SignedUrlResponse,
    TokenExpiredUploadError,
    UploadError,
    VerifyResult,
    _query_upload_offset,
    _resumable_upload_with_hash,
    _retry_transient,
    _streaming_upload_with_hash,
    get_allowed_filenames,
    get_resumable_session,
    match_local_files,
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
        self, mock_config_dir, mock_get_session, mock_stream, mock_complete, mock_verify, tmp_path
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
        self, mock_config_dir, mock_get_url, mock_get_session, mock_stream, mock_complete, mock_verify, tmp_path
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
    def test_failure_returns_error_result(self, mock_config_dir, mock_get_session, tmp_path):
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
    def test_resumed_upload_correct_hashes(self, mock_client_cls, mock_config_dir, tmp_path):
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

        result = verify_upload(PROJECT_ID, "sample.fastq.gz", local_md5_hex, 9, TOKEN, API_URL)

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

        result = verify_upload(PROJECT_ID, "sample.fastq.gz", "abcdef1234567890", 9, TOKEN, API_URL)

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
