"""Shared test fixtures for ee-metadata tests."""

from __future__ import annotations

import keyring
import pytest
from keyring.backend import KeyringBackend
from keyring.errors import PasswordDeleteError


class InMemoryKeyring(KeyringBackend):
    """In-memory keyring backend for testing."""

    priority = 10

    def __init__(self) -> None:
        self._store: dict[str, dict[str, str]] = {}

    def set_password(self, service: str, username: str, password: str) -> None:
        self._store.setdefault(service, {})[username] = password

    def get_password(self, service: str, username: str) -> str | None:
        return self._store.get(service, {}).get(username)

    def delete_password(self, service: str, username: str) -> None:
        try:
            del self._store[service][username]
        except KeyError:
            raise PasswordDeleteError(
                f"No password for {service}/{username}"
            ) from None


@pytest.fixture(autouse=True)
def _in_memory_keyring():
    """Install an in-memory keyring backend for every test."""
    backend = InMemoryKeyring()
    keyring.set_keyring(backend)
    yield backend
