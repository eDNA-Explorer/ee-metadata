"""Tests for the resume_store module."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from ee_metadata.resume_store import (
    SESSION_MAX_AGE_SECONDS,
    ResumeState,
    _state_key,
    clear_resume_state,
    load_resume_state,
    save_resume_state,
)


def _make_state(**overrides) -> ResumeState:
    defaults = {
        "session_uri": "https://storage.googleapis.com/upload/session/abc",
        "project_id": "proj-123",
        "filename": "sample.fastq.gz",
        "filesize": 1024,
        "file_mtime": 1700000000.0,
        "bytes_uploaded": 512,
        "sample_id": "s1",
        "file_id": "f1",
        "created_at": time.time(),
    }
    defaults.update(overrides)
    return ResumeState(**defaults)


class TestStateKey:
    def test_deterministic(self):
        """Same inputs always produce the same key."""
        k1 = _state_key("proj-123", "sample.fastq.gz")
        k2 = _state_key("proj-123", "sample.fastq.gz")
        assert k1 == k2

    def test_different_inputs_different_keys(self):
        """Different inputs produce different keys."""
        k1 = _state_key("proj-123", "sample.fastq.gz")
        k2 = _state_key("proj-456", "sample.fastq.gz")
        k3 = _state_key("proj-123", "other.fastq.gz")
        assert k1 != k2
        assert k1 != k3

    def test_key_length(self):
        """Key is exactly 16 hex characters."""
        k = _state_key("proj-123", "sample.fastq.gz")
        assert len(k) == 16
        assert all(c in "0123456789abcdef" for c in k)


class TestSaveAndLoad:
    @patch("ee_metadata.resume_store._config_dir")
    def test_save_and_load_roundtrip(self, mock_config_dir, tmp_path):
        mock_config_dir.return_value = tmp_path / "config"

        state = _make_state()
        save_resume_state(state)

        loaded = load_resume_state(
            state.project_id, state.filename, state.filesize, state.file_mtime
        )

        assert loaded is not None
        assert loaded.session_uri == state.session_uri
        assert loaded.bytes_uploaded == state.bytes_uploaded
        assert loaded.sample_id == state.sample_id
        assert loaded.file_id == state.file_id

    @patch("ee_metadata.resume_store._config_dir")
    def test_load_returns_none_when_no_state(self, mock_config_dir, tmp_path):
        mock_config_dir.return_value = tmp_path / "config"

        result = load_resume_state("proj-123", "nonexistent.fastq.gz", 1024, 1700000000.0)
        assert result is None

    @patch("ee_metadata.resume_store._config_dir")
    def test_invalidation_on_mtime_change(self, mock_config_dir, tmp_path):
        mock_config_dir.return_value = tmp_path / "config"

        state = _make_state(file_mtime=1700000000.0)
        save_resume_state(state)

        # Load with different mtime → should return None
        result = load_resume_state(
            state.project_id, state.filename, state.filesize, 1700000099.0
        )
        assert result is None

    @patch("ee_metadata.resume_store._config_dir")
    def test_invalidation_on_size_change(self, mock_config_dir, tmp_path):
        mock_config_dir.return_value = tmp_path / "config"

        state = _make_state(filesize=1024)
        save_resume_state(state)

        # Load with different filesize → should return None
        result = load_resume_state(
            state.project_id, state.filename, 2048, state.file_mtime
        )
        assert result is None

    @patch("ee_metadata.resume_store._config_dir")
    def test_invalidation_on_age(self, mock_config_dir, tmp_path):
        mock_config_dir.return_value = tmp_path / "config"

        old_time = time.time() - SESSION_MAX_AGE_SECONDS - 3600
        state = _make_state(created_at=old_time)
        save_resume_state(state)

        result = load_resume_state(
            state.project_id, state.filename, state.filesize, state.file_mtime
        )
        assert result is None

    @patch("ee_metadata.resume_store._config_dir")
    def test_corrupt_file_returns_none(self, mock_config_dir, tmp_path):
        mock_config_dir.return_value = tmp_path / "config"

        state = _make_state()
        save_resume_state(state)

        # Corrupt the file
        uploads_dir = tmp_path / "config" / "uploads"
        state_files = list(uploads_dir.glob("*.json"))
        assert len(state_files) == 1
        state_files[0].write_text("not valid json{{{")

        result = load_resume_state(
            state.project_id, state.filename, state.filesize, state.file_mtime
        )
        assert result is None


class TestClearResumeState:
    @patch("ee_metadata.resume_store._config_dir")
    def test_clear_removes_file(self, mock_config_dir, tmp_path):
        mock_config_dir.return_value = tmp_path / "config"

        state = _make_state()
        save_resume_state(state)

        clear_resume_state(state.project_id, state.filename)

        result = load_resume_state(
            state.project_id, state.filename, state.filesize, state.file_mtime
        )
        assert result is None

    @patch("ee_metadata.resume_store._config_dir")
    def test_clear_nonexistent_is_noop(self, mock_config_dir, tmp_path):
        mock_config_dir.return_value = tmp_path / "config"

        # Should not raise
        clear_resume_state("proj-123", "nonexistent.fastq.gz")
