"""Upload module for eDNA Explorer CLI.

Handles uploading FASTQ files to eDNA Explorer via pre-signed GCS URLs.
Supports byte-level resumable uploads for large FASTQ files.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from pathlib import Path
    from threading import Event

from ee_metadata.auth import AuthError, _clean_response_body

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UPLOAD_TIMEOUT = 3600.0  # 1 hour for large file uploads
API_TIMEOUT = 30.0
HASH_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0


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
    deduped: bool = False


@dataclass
class ChallengeRange:
    """A single byte range the server asked the client to hash."""

    offset: int
    length: int


@dataclass
class ClaimChecksum:
    """Response from the claim-by-checksum endpoint."""

    action: str  # 'upload' | 'challenge'
    reason: str | None = None
    token: str | None = None
    nonce: str | None = None
    ranges: list[ChallengeRange] | None = None
    expires_at: str | None = None


@dataclass
class ChallengeResult:
    """Response from the submit-checksum-challenge endpoint."""

    status: str  # 'linked'
    fastq_file_id: str
    verify_triggered: bool


@dataclass
class UploadSummary:
    """Summary of all uploads in a session."""

    succeeded: int
    failed: int
    results: list[UploadResult]


@dataclass
class ResumableSession:
    """Response from the upload-resumable-url endpoint."""

    session_uri: str
    sample_id: str
    file_id: str


@dataclass
class VerifyResult:
    """Result of post-upload integrity verification."""

    ok: bool
    remote_md5: str
    remote_size: int
    local_md5: str
    local_size: int


# ---------------------------------------------------------------------------
# API client functions
# ---------------------------------------------------------------------------


def _bearer_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _compute_file_sha256(filepath: Path) -> str:
    """Stream the whole file once and return the hex SHA-256."""
    h = hashlib.sha256()
    with filepath.open("rb") as fh:
        while True:
            chunk = fh.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _compute_challenge_mac(
    filepath: Path,
    nonce_b64url: str,
    ranges: list[ChallengeRange],
) -> str:
    """Compute hex HMAC-SHA256 over the requested byte ranges of *filepath*.

    Matches the server contract in
    `apps/web/server/api/routers/project/_vaultClaim.ts` — key is the raw
    32-byte base64url-decoded nonce, message is the concatenation of the
    bytes at each range in the order the server returned them.
    """
    key = _b64url_decode(nonce_b64url)
    h = hmac.new(key, digestmod=hashlib.sha256)
    with filepath.open("rb") as fh:
        for r in ranges:
            fh.seek(r.offset)
            remaining = r.length
            while remaining > 0:
                chunk = fh.read(min(remaining, HASH_CHUNK_SIZE))
                if not chunk:
                    raise UploadError(
                        f"short read at offset {r.offset} (need {r.length})"
                    )
                h.update(chunk)
                remaining -= len(chunk)
    return h.hexdigest()


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
            f"Server returned status {response.status_code}: "
            f"{_clean_response_body(response)}"
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
            f"Failed to get upload URL ({response.status_code}): "
            f"{_clean_response_body(response)}"
        )

    data = response.json()
    return SignedUrlResponse(
        signed_url=data["signedUrl"],
        sample_id=data["sampleId"],
        file_id=data["fileId"],
    )


def claim_by_checksum(
    project_metadata_id: str,
    sample_id: str,
    file_name: str,
    checksum: str,
    filesize: int,
    token: str,
    api_url: str,
) -> ClaimChecksum:
    """Step 1 of the FastQ-vault PoW dedup flow.

    Asks the server whether the SHA-256 is already in the vault. If it is,
    the server returns a signed byte-range challenge to prove possession;
    otherwise it returns ``action='upload'`` and the caller falls through
    to the normal upload path.

    Raises:
        TokenExpiredUploadError: On 401.
        UploadError: On other non-200 responses (caller should fall back).
        AuthError: On connection failures.
    """
    url = f"{api_url.rstrip('/')}/api/cli/claim-by-checksum"

    try:
        with httpx.Client(timeout=API_TIMEOUT) as client:
            response = client.post(
                url,
                headers=_bearer_headers(token),
                json={
                    "projectMetadataId": project_metadata_id,
                    "sampleId": sample_id,
                    "fileName": file_name,
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
            f"claim-by-checksum failed ({response.status_code}): "
            f"{_clean_response_body(response)}"
        )

    data = response.json()
    action = data.get("action", "upload")
    ranges: list[ChallengeRange] | None = None
    if action == "challenge":
        ranges = [
            ChallengeRange(offset=int(r["offset"]), length=int(r["length"]))
            for r in data.get("ranges", [])
        ]
    return ClaimChecksum(
        action=action,
        reason=data.get("reason"),
        token=data.get("token"),
        nonce=data.get("nonce"),
        ranges=ranges,
        expires_at=data.get("expiresAt"),
    )


def submit_checksum_challenge(
    challenge_token: str,
    mac: str,
    token: str,
    api_url: str,
) -> ChallengeResult:
    """Step 2 of the FastQ-vault PoW dedup flow.

    Submits the HMAC the client computed over the server-chosen byte
    ranges. On success the server links the project's ``ProjectFastqFile``
    row to the existing vault object, and the upload is skipped.

    Raises:
        TokenExpiredUploadError: On 401.
        UploadError: On 403 (denied — replay/bad MAC/stale target/expired)
            or any other non-200. Callers should treat 403 as a signal to
            fall back to a normal upload.
        AuthError: On connection failures.
    """
    url = f"{api_url.rstrip('/')}/api/cli/submit-checksum-challenge"

    try:
        with httpx.Client(timeout=API_TIMEOUT) as client:
            response = client.post(
                url,
                headers=_bearer_headers(token),
                json={"token": challenge_token, "mac": mac},
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
        raise UploadError(
            "Vault dedup challenge denied; will retry as a normal upload."
        )
    if response.status_code != 200:
        raise UploadError(
            f"submit-checksum-challenge failed ({response.status_code}): "
            f"{_clean_response_body(response)}"
        )

    data = response.json()
    return ChallengeResult(
        status=data.get("status", "linked"),
        fastq_file_id=data.get("fastqFileId", ""),
        verify_triggered=bool(data.get("verifyTriggered", False)),
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
            f"Upload completion failed ({response.status_code}): "
            f"{_clean_response_body(response)}"
        )


def get_resumable_session(
    project_id: str, filename: str, token: str, api_url: str
) -> ResumableSession:
    """Request a resumable upload session URI from the server.

    Raises:
        UploadError: On HTTP errors (including 404 if server doesn't support resumable).
        AuthError: On connection failures.
    """
    url = f"{api_url.rstrip('/')}/api/cli/upload-resumable-url"

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
    if response.status_code == 404:
        raise UploadError(
            f"Resumable upload not available ({response.status_code}): "
            f"{_clean_response_body(response)}"
        )
    if response.status_code != 200:
        raise UploadError(
            f"Failed to get resumable session ({response.status_code}): "
            f"{_clean_response_body(response)}"
        )

    data = response.json()
    return ResumableSession(
        session_uri=data["sessionUri"],
        sample_id=data["sampleId"],
        file_id=data["fileId"],
    )


def verify_upload(
    project_id: str,
    filename: str,
    expected_md5: str,
    expected_size: int,
    token: str,
    api_url: str,
) -> VerifyResult:
    """Verify upload integrity by comparing local hashes against GCS metadata.

    Raises:
        UploadError: On HTTP errors or integrity mismatch.
        AuthError: On connection failures.
    """
    url = f"{api_url.rstrip('/')}/api/cli/upload-verify"

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
    if response.status_code == 404:
        raise UploadError("Uploaded file not found in GCS for verification.")
    if response.status_code != 200:
        raise UploadError(
            f"Verify request failed ({response.status_code}): "
            f"{_clean_response_body(response)}"
        )

    data = response.json()
    remote_md5 = data["md5Hash"]
    remote_size = int(data["size"])

    # GCS stores MD5 as base64; convert our hex digest for comparison
    local_md5_b64 = base64.b64encode(bytes.fromhex(expected_md5)).decode()

    ok = remote_md5 == local_md5_b64 and remote_size == expected_size

    return VerifyResult(
        ok=ok,
        remote_md5=remote_md5,
        remote_size=remote_size,
        local_md5=local_md5_b64,
        local_size=expected_size,
    )


# ---------------------------------------------------------------------------
# Resumable upload helpers
# ---------------------------------------------------------------------------


def _query_upload_offset(session_uri: str, filesize: int) -> int:
    """Query GCS for the last confirmed byte offset of a resumable session.

    Sends ``PUT`` with ``Content-Range: bytes */<filesize>`` to the session URI.
    A 308 response with a ``Range`` header means partial upload; we parse the
    last byte from ``Range: bytes=0-<last>``.
    A 200/201 means the upload is already complete.
    Any other response means the session is invalid → return 0.
    """
    try:
        with httpx.Client(timeout=API_TIMEOUT) as client:
            response = client.put(
                session_uri,
                headers={"Content-Range": f"bytes */{filesize}"},
                content=b"",
            )
    except (httpx.TimeoutException, httpx.RequestError):
        return 0

    if response.status_code == 308:
        range_header = response.headers.get("Range", "")
        # Format: bytes=0-<last_byte>
        if range_header.startswith("bytes=0-"):
            try:
                return int(range_header.split("-")[1]) + 1
            except (IndexError, ValueError):
                return 0
        return 0

    if response.status_code in (200, 201):
        # Upload is already complete
        return filesize

    # Session expired or invalid
    return 0


def _resumable_upload_with_hash(
    filepath: Path,
    session_uri: str,
    project_id: str,
    filesize: int,
    resume_offset: int = 0,
    progress_callback: ProgressCallback | None = None,
    sample_id: str = "",
    file_id: str = "",
    cancel_event: Event | None = None,
) -> tuple[str, str, int]:
    """Upload a file in chunks using the GCS resumable upload protocol.

    Computes SHA-256 and MD5 over the entire file (re-reading from the start
    on resume to restore hash state).

    Returns:
        (sha256_hex, md5_hex, filesize) tuple.

    Raises:
        UploadError: If a chunk PUT fails permanently.
    """
    from ee_metadata.resume_store import ResumeState, save_resume_state

    sha256_hasher = hashlib.sha256()
    md5_hasher = hashlib.md5(usedforsecurity=False)

    with filepath.open("rb") as fh:
        # Re-read already-uploaded bytes to restore hash state
        if resume_offset > 0:
            remaining = resume_offset
            while remaining > 0:
                chunk = fh.read(min(UPLOAD_CHUNK_SIZE, remaining))
                if not chunk:
                    break
                sha256_hasher.update(chunk)
                md5_hasher.update(chunk)
                remaining -= len(chunk)

            # Advance progress bar to show resumed position
            if progress_callback:
                progress_callback(resume_offset)

        # Upload remaining chunks
        offset = resume_offset
        while offset < filesize:
            if cancel_event is not None and cancel_event.is_set():
                raise UploadError(f"Upload cancelled for {filepath.name}")

            chunk_end = min(offset + UPLOAD_CHUNK_SIZE, filesize)
            chunk = fh.read(chunk_end - offset)
            if not chunk:
                break

            sha256_hasher.update(chunk)
            md5_hasher.update(chunk)

            # Content-Range: bytes {start}-{end-1}/{total}
            content_range = f"bytes {offset}-{chunk_end - 1}/{filesize}"

            try:
                with httpx.Client(timeout=UPLOAD_TIMEOUT) as client:
                    response = client.put(
                        session_uri,
                        content=chunk,
                        headers={
                            "Content-Range": content_range,
                            "Content-Type": "application/octet-stream",
                        },
                    )
            except httpx.TimeoutException as e:
                raise UploadError(
                    f"Upload timed out for {filepath.name} at offset {offset}"
                ) from e
            except httpx.RequestError as e:
                raise UploadError(
                    f"Upload failed for {filepath.name} at offset {offset}: {e}"
                ) from e

            # 308 = chunk accepted, continue; 200/201 = upload complete
            if response.status_code not in (200, 201, 308):
                raise UploadError(
                    f"GCS resumable upload failed for {filepath.name} "
                    f"(status {response.status_code}) at offset {offset}"
                )

            offset = chunk_end

            if progress_callback:
                progress_callback(len(chunk))

            # Persist resume state after each chunk (only for intermediate)
            if offset < filesize:
                save_resume_state(
                    ResumeState(
                        session_uri=session_uri,
                        project_id=project_id,
                        filename=filepath.name,
                        filesize=filesize,
                        file_mtime=filepath.stat().st_mtime,
                        bytes_uploaded=offset,
                        sample_id=sample_id,
                        file_id=file_id,
                        created_at=time.time(),
                    )
                )

    return sha256_hasher.hexdigest(), md5_hasher.hexdigest(), filesize


# ---------------------------------------------------------------------------
# Streaming upload with inline SHA-256
# ---------------------------------------------------------------------------

ProgressCallback = Callable[[int], None]


def _streaming_upload_with_hash(
    filepath: Path,
    signed_url: str,
    progress_callback: ProgressCallback | None = None,
) -> tuple[str, str, int]:
    """Upload a file to a pre-signed URL while computing SHA-256 and MD5.

    Reads the file once, feeding each chunk to both hashers and the
    upload stream. Peak memory usage is ~UPLOAD_CHUNK_SIZE per upload.

    Returns:
        (sha256_hex, md5_hex, filesize) tuple.

    Raises:
        UploadError: If the PUT request fails.
    """
    filesize = filepath.stat().st_size
    sha256_hasher = hashlib.sha256()
    md5_hasher = hashlib.md5(usedforsecurity=False)

    def _chunk_generator():
        with filepath.open("rb") as fh:
            while True:
                chunk = fh.read(UPLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                sha256_hasher.update(chunk)
                md5_hasher.update(chunk)
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

    return sha256_hasher.hexdigest(), md5_hasher.hexdigest(), filesize


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

    unmatched_server = [
        af for af in allowed if af.file_name not in matched_server_files
    ]

    return MatchResult(
        matched=matched,
        already_uploaded=already_uploaded,
        needs_reupload=needs_reupload,
        unmatched_local=unmatched_local,
        unmatched_server=unmatched_server,
    )


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------


def _retry_transient(
    fn: Callable,
    *args,
    max_retries: int = MAX_RETRIES,
    cancel_event: Event | None = None,
    **kwargs,
):
    """Retry *fn* on transient errors with exponential backoff.

    Never retries ``TokenExpiredUploadError`` (401) or client errors
    containing "permission" / "not found" (403/404). Bails immediately
    if *cancel_event* is set.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        if cancel_event is not None and cancel_event.is_set():
            raise TokenExpiredUploadError("Upload cancelled: token expired.")
        try:
            return fn(*args, **kwargs)
        except TokenExpiredUploadError:
            raise
        except UploadError as e:
            msg = str(e).lower()
            if "permission" in msg or "not found" in msg:
                raise
            last_exc = e
        except AuthError as e:
            last_exc = e
        if attempt < max_retries:
            delay = min(RETRY_BASE_DELAY * (2**attempt), RETRY_MAX_DELAY)
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


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
    cancel_event: Event | None = None,
) -> UploadResult:
    """Upload a single file with resumable upload support and fallback.

    Flow:
    1. Check for existing resume state → resume if valid
    2. Try resumable upload (new session from server)
    3. Fallback to signed-URL upload if server doesn't support resumable
    4. Post-upload integrity verification
    5. Notify server of completion

    This function is designed to be called from a thread pool.
    When *cancel_event* is set (by any thread detecting a 401), remaining
    files are skipped immediately rather than failing one-by-one.
    """
    from ee_metadata.resume_store import (
        clear_resume_state,
        load_resume_state,
        save_resume_state,
    )
    from ee_metadata.resume_store import ResumeState

    filename = filepath.name

    # Circuit breaker: skip immediately if another thread tripped the flag
    if cancel_event is not None and cancel_event.is_set():
        return UploadResult(filename=filename, success=False, skipped=True)

    try:
        filesize = filepath.stat().st_size
        file_mtime = filepath.stat().st_mtime
        session_uri: str | None = None
        sample_id: str = ""
        file_id: str = ""
        resume_offset = 0
        use_resumable = True

        # 1. Check for existing resume state
        existing = load_resume_state(project_id, filename, filesize, file_mtime)
        if existing is not None:
            # Validate the session is still alive on GCS
            confirmed = _query_upload_offset(existing.session_uri, filesize)
            if confirmed > 0 and confirmed < filesize:
                session_uri = existing.session_uri
                sample_id = existing.sample_id
                file_id = existing.file_id
                resume_offset = confirmed
                log.info(
                    "Resuming %s from %d / %d bytes",
                    filename,
                    resume_offset,
                    filesize,
                )
            elif confirmed >= filesize:
                # Upload already complete on GCS — skip to verification
                session_uri = existing.session_uri
                sample_id = existing.sample_id
                file_id = existing.file_id
                resume_offset = filesize
            else:
                # Session expired or invalid
                clear_resume_state(project_id, filename)

        # 1.5. Vault dedup pre-check (FastQ-vault PoW protocol).
        # When the SHA-256 is already in the vault, the server returns a
        # signed byte-range challenge; we answer it to prove possession
        # and skip the upload entirely. Only attempted when there's no
        # in-flight resumable session — if bytes are already on the wire,
        # finish that upload rather than throwing away the work.
        if session_uri is None:
            sha256_hex = _compute_file_sha256(filepath)
            try:
                claim = claim_by_checksum(
                    project_metadata_id=project_metadata_id,
                    sample_id=allowed_file.sample_id,
                    file_name=filename,
                    checksum=sha256_hex,
                    filesize=filesize,
                    token=token,
                    api_url=api_url,
                )
            except TokenExpiredUploadError:
                raise
            except (UploadError, AuthError) as e:
                log.info(
                    "claim-by-checksum unavailable for %s (%s); falling back to upload",
                    filename,
                    e,
                )
                claim = ClaimChecksum(action="upload", reason="claim_failed")

            if (
                claim.action == "challenge"
                and claim.token
                and claim.nonce
                and claim.ranges
            ):
                try:
                    mac_hex = _compute_challenge_mac(
                        filepath, claim.nonce, claim.ranges
                    )
                    submit_checksum_challenge(
                        challenge_token=claim.token,
                        mac=mac_hex,
                        token=token,
                        api_url=api_url,
                    )
                    if progress_callback:
                        progress_callback(filesize)
                    return UploadResult(
                        filename=filename,
                        success=True,
                        checksum=sha256_hex,
                        filesize=filesize,
                        deduped=True,
                    )
                except TokenExpiredUploadError:
                    raise
                except (UploadError, AuthError) as e:
                    log.info(
                        "Vault dedup challenge failed for %s (%s); "
                        "falling back to normal upload",
                        filename,
                        e,
                    )

        # 2. If no resume state, request a new resumable session
        if session_uri is None:
            try:
                session = _retry_transient(
                    get_resumable_session,
                    project_id,
                    filename,
                    token,
                    api_url,
                    cancel_event=cancel_event,
                )
                session_uri = session.session_uri
                sample_id = session.sample_id
                file_id = session.file_id

                # Save initial resume state
                save_resume_state(
                    ResumeState(
                        session_uri=session_uri,
                        project_id=project_id,
                        filename=filename,
                        filesize=filesize,
                        file_mtime=file_mtime,
                        bytes_uploaded=0,
                        sample_id=sample_id,
                        file_id=file_id,
                        created_at=time.time(),
                    )
                )
            except UploadError as e:
                if "not available" in str(e).lower() or "404" in str(e):
                    # Server doesn't support resumable uploads — fallback
                    use_resumable = False
                    log.info(
                        "Server does not support resumable uploads for %s, "
                        "falling back to signed URL",
                        filename,
                    )
                else:
                    raise

        # 3. Upload the file
        if use_resumable and session_uri is not None:
            if resume_offset >= filesize:
                # Already complete on GCS — re-read file to compute hashes
                sha256_hasher = hashlib.sha256()
                md5_hasher = hashlib.md5(usedforsecurity=False)
                with filepath.open("rb") as fh:
                    while True:
                        chunk = fh.read(UPLOAD_CHUNK_SIZE)
                        if not chunk:
                            break
                        sha256_hasher.update(chunk)
                        md5_hasher.update(chunk)
                        if progress_callback:
                            progress_callback(len(chunk))
                checksum = sha256_hasher.hexdigest()
                md5_hex = md5_hasher.hexdigest()
            else:
                checksum, md5_hex, filesize = _resumable_upload_with_hash(
                    filepath,
                    session_uri,
                    project_id,
                    filesize,
                    resume_offset,
                    progress_callback,
                    sample_id=sample_id,
                    file_id=file_id,
                    cancel_event=cancel_event,
                )
        else:
            # Fallback: signed URL upload
            signed = _retry_transient(
                get_signed_url,
                project_id,
                filename,
                token,
                api_url,
                cancel_event=cancel_event,
            )
            sample_id = signed.sample_id
            file_id = signed.file_id
            checksum, md5_hex, filesize = _streaming_upload_with_hash(
                filepath, signed.signed_url, progress_callback
            )

        # 4. Check cancel_event after upload — verification and completion need JWT
        if cancel_event is not None and cancel_event.is_set():
            return UploadResult(filename=filename, success=False, skipped=True)

        # 5. Post-upload integrity verification
        try:
            vr = _retry_transient(
                verify_upload,
                project_id,
                filename,
                md5_hex,
                filesize,
                token,
                api_url,
                cancel_event=cancel_event,
            )
            if not vr.ok:
                log.warning(
                    "Integrity mismatch for %s: local_md5=%s remote_md5=%s "
                    "local_size=%d remote_size=%d",
                    filename,
                    vr.local_md5,
                    vr.remote_md5,
                    vr.local_size,
                    vr.remote_size,
                )
                clear_resume_state(project_id, filename)
                return UploadResult(
                    filename=filename,
                    success=False,
                    error="Integrity verification failed: uploaded file does not match local file.",
                )
        except UploadError as e:
            if "not found" in str(e).lower() or "404" in str(e):
                # Server doesn't support verify endpoint — skip verification
                log.info(
                    "Verify endpoint not available, skipping verification for %s",
                    filename,
                )
            else:
                raise

        # 6. Check cancel_event before completion
        if cancel_event is not None and cancel_event.is_set():
            return UploadResult(filename=filename, success=False, skipped=True)

        # 7. Notify server of completion (with retry for transient errors)
        _retry_transient(
            complete_upload,
            project_metadata_id=project_metadata_id,
            sample_id=sample_id,
            filename=filename,
            checksum=checksum,
            filesize=filesize,
            token=token,
            api_url=api_url,
            cancel_event=cancel_event,
        )

        # Only clear resume state after everything succeeded
        clear_resume_state(project_id, filename)

    except TokenExpiredUploadError as e:
        # Trip the circuit breaker for all threads
        if cancel_event is not None:
            cancel_event.set()
        return UploadResult(filename=filename, success=False, error=str(e))
    except (UploadError, AuthError) as e:
        return UploadResult(filename=filename, success=False, error=str(e))

    return UploadResult(
        filename=filename,
        success=True,
        checksum=checksum,
        filesize=filesize,
    )
