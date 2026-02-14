"""
Memory-based client data store for development and testing.

Stores serialized data in a local Python dictionary. Not suitable for
production — cache is not shared between processes.

Key components:
- MemClientDataStore: In-memory dict-backed client data store
"""

from typing import TYPE_CHECKING

from pynenc.client_data_store.base_client_data_store import BaseClientDataStore

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class MemClientDataStore(BaseClientDataStore):
    """
    Memory-based implementation of the client data store.

    Stores serialized values in local memory using a dictionary.
    Suitable for development and testing, but not recommended for
    production as cache is not shared between processes.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._storage: dict[str, str] = {}

    def _store(self, key: str, value: str) -> None:
        """Store a value in memory."""
        self._storage[key] = value

    def _retrieve(self, key: str) -> str:
        """Retrieve a value from memory."""
        if key not in self._storage:
            raise KeyError(f"Key not found: {key}")
        return self._storage[key]

    def _purge(self) -> None:
        """Clear the memory store."""
        self._storage.clear()
