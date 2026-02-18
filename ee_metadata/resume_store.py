"""Persistent resume state for resumable uploads.

Stores upload progress as JSON files in the config directory so that
interrupted uploads can be resumed from the last confirmed byte offset.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

from ee_metadata.token_storage import _config_dir

# GCS resumable sessions are valid for ~1 week; expire after 6 days to be safe
SESSION_MAX_AGE_SECONDS = 6 * 24 * 3600  # 6 days


@dataclass
class ResumeState:
    """Persisted state for a resumable upload."""

    session_uri: str
    project_id: str
    filename: str
    filesize: int
    file_mtime: float
    bytes_uploaded: int
    sample_id: str
    file_id: str
    created_at: float


def _uploads_dir() -> Path:
    """Return the directory used to store resume state files."""
    return _config_dir() / "uploads"


def _state_key(project_id: str, filename: str) -> str:
    """Deterministic short key from project_id + filename."""
    raw = f"{project_id}:{filename}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _state_path(project_id: str, filename: str) -> Path:
    """Return the path to the resume state file for a given upload."""
    return _uploads_dir() / f"{_state_key(project_id, filename)}.json"


def save_resume_state(state: ResumeState) -> None:
    """Persist resume state to disk."""
    path = _state_path(state.project_id, state.filename)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(json.dumps(asdict(state), indent=2))


def load_resume_state(
    project_id: str, filename: str, filesize: int, file_mtime: float
) -> ResumeState | None:
    """Load and validate resume state.

    Returns None if:
    - No saved state exists
    - File mtime or size changed (local file was modified)
    - Session is older than 6 days (GCS session likely expired)
    """
    path = _state_path(project_id, filename)
    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text())
        state = ResumeState(**data)
    except (json.JSONDecodeError, KeyError, TypeError):
        # Corrupt state file — remove and start fresh
        path.unlink(missing_ok=True)
        return None

    # Invalidate if file was modified
    if state.filesize != filesize or abs(state.file_mtime - file_mtime) > 0.001:
        path.unlink(missing_ok=True)
        return None

    # Invalidate if session is too old
    if time.time() - state.created_at > SESSION_MAX_AGE_SECONDS:
        path.unlink(missing_ok=True)
        return None

    return state


def clear_resume_state(project_id: str, filename: str) -> None:
    """Remove resume state for a given upload."""
    path = _state_path(project_id, filename)
    path.unlink(missing_ok=True)
