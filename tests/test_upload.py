"""Tests for the upload module."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from ee_metadata.auth import AuthError
from ee_metadata.upload import (
    AllowedFile,
    ProjectUploadInfo,
    SignedUrlResponse,
    TokenExpiredUploadError,
    UploadError,
    _streaming_upload_with_hash,
    get_allowed_filenames,
    match_local_files,
    upload_file,
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

        sha, size = _streaming_upload_with_hash(
            filepath, "https://gcs.example.com/upload"
        )

        assert sha == expected_hash
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
    @patch("ee_metadata.upload.complete_upload")
    @patch("ee_metadata.upload._streaming_upload_with_hash")
    @patch("ee_metadata.upload.get_signed_url")
    def test_success_flow(self, mock_get_url, mock_stream, mock_complete):
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")

        mock_get_url.return_value = SignedUrlResponse(
            signed_url="https://gcs.example.com/upload",
            sample_id="s1",
            file_id="f1",
        )
        mock_stream.return_value = ("abc123hash", 4)

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

    @patch("ee_metadata.upload.get_signed_url")
    def test_failure_returns_error_result(self, mock_get_url):
        filepath = _tmp_file(b"data")
        af = _make_allowed(filepath.name, "s1")

        mock_get_url.side_effect = UploadError("no permission")

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
