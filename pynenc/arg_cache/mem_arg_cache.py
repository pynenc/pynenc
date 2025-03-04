from typing import TYPE_CHECKING

from pynenc.arg_cache.base_arg_cache import BaseArgCache

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class MemArgCache(BaseArgCache):
    """
    Memory-based implementation of argument caching.

    Stores serialized arguments in local memory using a dictionary.
    This implementation is suitable for development and testing,
    but not recommended for production as cache is not shared between processes.
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
            raise KeyError(f"Cache key not found: {key}")
        return self._storage[key]

    def _purge(self) -> None:
        """Clear the memory cache."""
        self._storage.clear()
