"""Upload module for eDNA Explorer CLI.

Handles uploading FASTQ files to eDNA Explorer via pre-signed GCS URLs.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from pathlib import Path

from ee_metadata.auth import AuthError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UPLOAD_TIMEOUT = 3600.0  # 1 hour for large file uploads
API_TIMEOUT = 30.0
HASH_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class UploadError(Exception):
    """Base exception for upload failures."""


class TokenExpiredUploadError(UploadError):
    """Raised when an API call during upload returns 401 (token expired)."""


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class AllowedFile:
    """A file the server expects for this project."""

    normalized_name: str
    file_name: str
    sample_id: str
    uploaded: bool
    md5_checksum: str | None
    note_type: str | None = None


@dataclass
class ProjectUploadInfo:
    """Response from the allowed-filenames endpoint."""

    allowed_files: list[AllowedFile]
    project_metadata_id: str


@dataclass
class SignedUrlResponse:
    """Response from the upload-url endpoint."""

    signed_url: str
    sample_id: str
    file_id: str


@dataclass
class UploadResult:
    """Result of a single file upload."""

    filename: str
    success: bool
    error: str | None = None
    checksum: str | None = None
    filesize: int = 0
    skipped: bool = False


@dataclass
class UploadSummary:
    """Summary of all uploads in a session."""

    succeeded: int
    failed: int
    results: list[UploadResult]


# ---------------------------------------------------------------------------
# API client functions
# ---------------------------------------------------------------------------


def _bearer_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def get_allowed_filenames(
    project_id: str, token: str, api_url: str
) -> ProjectUploadInfo:
    """Fetch the list of filenames the server expects for a project.

    Raises:
        UploadError: On HTTP errors (401/403/404/other).
        AuthError: On connection failures.
    """
    url = f"{api_url.rstrip('/')}/api/cli/allowed-filenames/{project_id}"

    try:
        with httpx.Client(timeout=API_TIMEOUT) as client:
            response = client.get(url, headers=_bearer_headers(token))
    except httpx.TimeoutException as e:
        raise AuthError(f"Request timed out connecting to {api_url}") from e
    except httpx.RequestError as e:
        raise AuthError(f"Failed to connect to {api_url}: {e}") from e

    if response.status_code == 401:
        raise UploadError("Authentication failed. Run 'ee-metadata login' again.")
    if response.status_code == 403:
        raise UploadError("You don't have permission to upload to this project.")
    if response.status_code == 404:
        raise UploadError(f"Project '{project_id}' not found.")
    if response.status_code != 200:
        raise UploadError(
            f"Server returned status {response.status_code}: {response.text}"
        )

    data = response.json()
    allowed = [
        AllowedFile(
            normalized_name=f.get("normalizedName", ""),
            file_name=f.get("fileName", ""),
            sample_id=f.get("sampleId", ""),
            uploaded=f.get("uploaded", False),
            md5_checksum=f.get("md5CheckSum"),
            note_type=f.get("noteType"),
        )
        for f in data.get("allowedFilenames", [])
    ]

    return ProjectUploadInfo(
        allowed_files=allowed,
        project_metadata_id=data.get("projectMetadataId", ""),
    )


def get_signed_url(
    project_id: str, filename: str, token: str, api_url: str
) -> SignedUrlResponse:
    """Request a pre-signed GCS upload URL for a single file.

    Raises:
        UploadError: On HTTP errors.
        AuthError: On connection failures.
    """
    url = f"{api_url.rstrip('/')}/api/cli/upload-url"

    try:
        with httpx.Client(timeout=API_TIMEOUT) as client:
            response = client.post(
                url,
                headers=_bearer_headers(token),
                json={"projectId": project_id, "filename": filename},
            )
    except httpx.TimeoutException as e:
        raise AuthError(f"Request timed out connecting to {api_url}") from e
    except httpx.RequestError as e:
        raise AuthError(f"Failed to connect to {api_url}: {e}") from e

    if response.status_code == 401:
        raise TokenExpiredUploadError(
            "Token expired during upload. Run 'ee-metadata login' again."
        )
    if response.status_code == 403:
        raise UploadError("You don't have permission to upload to this project.")
    if response.status_code != 200:
        raise UploadError(
            f"Failed to get upload URL ({response.status_code}): {response.text}"
        )

    data = response.json()
    return SignedUrlResponse(
        signed_url=data["signedUrl"],
        sample_id=data["sampleId"],
        file_id=data["fileId"],
    )


def complete_upload(
    project_metadata_id: str,
    sample_id: str,
    filename: str,
    checksum: str,
    filesize: int,
    token: str,
    api_url: str,
) -> None:
    """Notify the server that a file upload is complete.

    Raises:
        UploadError: On HTTP errors.
        AuthError: On connection failures.
    """
    url = f"{api_url.rstrip('/')}/api/cli/upload-complete"

    try:
        with httpx.Client(timeout=API_TIMEOUT) as client:
            response = client.post(
                url,
                headers=_bearer_headers(token),
                json={
                    "projectMetadataId": project_metadata_id,
                    "sampleId": sample_id,
                    "fileName": filename,
                    "checksum": checksum,
                    "filesize": filesize,
                },
            )
    except httpx.TimeoutException as e:
        raise AuthError(f"Request timed out connecting to {api_url}") from e
    except httpx.RequestError as e:
        raise AuthError(f"Failed to connect to {api_url}: {e}") from e

    if response.status_code == 401:
        raise TokenExpiredUploadError(
            "Token expired during upload. Run 'ee-metadata login' again."
        )
    if response.status_code != 200:
        raise UploadError(
            f"Upload completion failed ({response.status_code}): {response.text}"
        )


# ---------------------------------------------------------------------------
# Streaming upload with inline SHA-256
# ---------------------------------------------------------------------------

ProgressCallback = Callable[[int], None]


def _streaming_upload_with_hash(
    filepath: Path,
    signed_url: str,
    progress_callback: ProgressCallback | None = None,
) -> tuple[str, int]:
    """Upload a file to a pre-signed URL while computing its SHA-256 hash.

    Reads the file once, feeding each chunk to both the hasher and the
    upload stream. Peak memory usage is ~UPLOAD_CHUNK_SIZE per upload.

    Returns:
        (sha256_hex, filesize) tuple.

    Raises:
        UploadError: If the PUT request fails.
    """
    filesize = filepath.stat().st_size
    hasher = hashlib.sha256()

    def _chunk_generator():
        with filepath.open("rb") as fh:
            while True:
                chunk = fh.read(UPLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)
                if progress_callback:
                    progress_callback(len(chunk))
                yield chunk

    try:
        with httpx.Client(timeout=UPLOAD_TIMEOUT) as client:
            response = client.put(
                signed_url,
                content=_chunk_generator(),
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(filesize),
                },
            )
    except httpx.TimeoutException as e:
        raise UploadError(f"Upload timed out for {filepath.name}") from e
    except httpx.RequestError as e:
        raise UploadError(f"Upload failed for {filepath.name}: {e}") from e

    if response.status_code not in (200, 201):
        raise UploadError(
            f"GCS upload failed for {filepath.name} (status {response.status_code})"
        )

    return hasher.hexdigest(), filesize


# ---------------------------------------------------------------------------
# File matching
# ---------------------------------------------------------------------------


@dataclass
class MatchResult:
    """Result of matching local files against server-allowed files."""

    matched: list[tuple[Path, AllowedFile]]
    already_uploaded: list[tuple[Path, AllowedFile]]
    needs_reupload: list[tuple[Path, AllowedFile]]
    unmatched_local: list[Path]
    unmatched_server: list[AllowedFile]


def match_local_files(
    local_files: list[Path], allowed: list[AllowedFile]
) -> MatchResult:
    """Match local FASTQ files against the server's allowed file list.

    Matches by file_name first, then by normalized_name.
    """
    matched: list[tuple[Path, AllowedFile]] = []
    already_uploaded: list[tuple[Path, AllowedFile]] = []
    needs_reupload: list[tuple[Path, AllowedFile]] = []
    unmatched_local: list[Path] = []

    # Index allowed files by both name forms for O(1) lookup
    by_filename: dict[str, AllowedFile] = {}
    by_normalized: dict[str, AllowedFile] = {}
    for af in allowed:
        if af.file_name:
            by_filename[af.file_name] = af
        if af.normalized_name:
            by_normalized[af.normalized_name] = af

    matched_server_files: set[str] = set()

    for local_path in local_files:
        name = local_path.name
        af = by_filename.get(name) or by_normalized.get(name)

        if af is None:
            unmatched_local.append(local_path)
        elif af.uploaded and af.note_type == "REUPLOAD":
            needs_reupload.append((local_path, af))
            matched_server_files.add(af.file_name)
        elif af.uploaded:
            already_uploaded.append((local_path, af))
            matched_server_files.add(af.file_name)
        else:
            matched.append((local_path, af))
            matched_server_files.add(af.file_name)

    unmatched_server = [af for af in allowed if af.file_name not in matched_server_files]

    return MatchResult(
        matched=matched,
        already_uploaded=already_uploaded,
        needs_reupload=needs_reupload,
        unmatched_local=unmatched_local,
        unmatched_server=unmatched_server,
    )


# ---------------------------------------------------------------------------
# Per-file upload orchestration
# ---------------------------------------------------------------------------


def upload_file(
    filepath: Path,
    allowed_file: AllowedFile,
    project_id: str,
    project_metadata_id: str,
    token: str,
    api_url: str,
    progress_callback: ProgressCallback | None = None,
) -> UploadResult:
    """Upload a single file: get signed URL → stream upload → complete.

    This function is designed to be called from a thread pool.
    """
    filename = filepath.name

    try:
        # 1. Get a signed upload URL (just-in-time to avoid expiry)
        signed = get_signed_url(project_id, filename, token, api_url)

        # 2. Stream upload while computing hash
        checksum, filesize = _streaming_upload_with_hash(
            filepath, signed.signed_url, progress_callback
        )

        # 3. Notify server of completion
        complete_upload(
            project_metadata_id=project_metadata_id,
            sample_id=signed.sample_id,
            filename=filename,
            checksum=checksum,
            filesize=filesize,
            token=token,
            api_url=api_url,
        )

    except (UploadError, AuthError) as e:
        return UploadResult(filename=filename, success=False, error=str(e))

    return UploadResult(
        filename=filename,
        success=True,
        checksum=checksum,
        filesize=filesize,
    )
