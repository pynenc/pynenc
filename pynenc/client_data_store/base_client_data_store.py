"""
Base class and interface for the ClientDataStore system.

Manages serialization and external storage of client-provided data:
task arguments, results, and exceptions. Small values pass through as inline
serialized strings. Large values are stored externally with content-hash keys
for automatic deduplication.

Key components:
- BaseClientDataStore: Abstract base with serialize/deserialize and size-based routing
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pynenc.conf.config_client_data_store import ConfigClientDataStore
from pynenc.exceptions import SerializationError
from pynenc.serializer.constants import ReservedKeys

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class BaseClientDataStore(ABC):
    """
    Manages serialization and storage of client-provided data.

    Handles task arguments, results, and exceptions. Small values are returned
    inline as serialized strings. Large values are stored externally and
    referenced by content-hash keys for automatic deduplication.

    Deduplication is achieved through deterministic content-hashing: the same
    serialized value always produces the same SHA-256 key, so backends
    naturally deduplicate via INSERT OR REPLACE / upsert semantics.

    A small process-local LRU cache avoids repeated backend reads for recently
    deserialized objects.

    Subclasses implement three abstract methods for backend storage:
    ``_store``, ``_retrieve``, and ``_purge``.
    """

    def __init__(self, app: "Pynenc") -> None:
        """
        Initialize with app reference.

        :param Pynenc app: The Pynenc application instance
        """
        self.app = app
        self._logger = logging.getLogger(
            f"pynenc.client_data_store.{type(self).__name__}"
        )
        self._deserialized_cache: OrderedDict[str, Any] = OrderedDict()

    # ── Configuration ──────────────────────────────────────────────

    @cached_property
    def conf(self) -> ConfigClientDataStore:
        """Get the client data store configuration."""
        return ConfigClientDataStore(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    # ── Public API ─────────────────────────────────────────────────

    def serialize_arguments(
        self, kwargs: dict[str, Any], disable_cache_args: tuple[str, ...]
    ) -> dict[str, str]:
        """Serialize task arguments, externalizing large values.

        The ``disable_cache_args`` config controls which arguments always
        stay inline (never externalized). Use ``("*",)`` to disable
        external storage for all arguments.

        Deduplication of identical values is handled by content-hash keys:
        the same serialized content always maps to the same storage key.

        :param dict[str, Any] kwargs: The dictionary with raw Python values.
        :param tuple[str, ...] disable_cache_args: Argument names to skip external storage.
        :return: Dict of argument name → serialized value (inline or reference key).
        """
        disable_all = "*" in disable_cache_args
        result: dict[str, str] = {}
        for key, value in kwargs.items():
            try:
                result[key] = self.serialize(
                    value, disable_cache=(disable_all or key in disable_cache_args)
                )
            except (TypeError, ValueError) as exc:
                truncated = repr(value)[:200]
                raise SerializationError(
                    f"Failed to serialize argument '{key}' "
                    f"(type={type(value).__qualname__}): {exc}\n"
                    f"  value (truncated): {truncated}"
                ) from exc
        return result

    def deserialize_arguments(self, serialized_args: dict[str, str]) -> dict[str, Any]:
        """Deserialize argument values, resolving any external references.

        Each value is checked: if it is a ClientDataStore reference key,
        the value is loaded from external storage first, then deserialized.
        Inline values are deserialized directly.

        :param dict[str, str] serialized_args: Argument name → serialized value.
        :return: Dict of argument name → deserialized Python object.
        """
        return {k: self.resolve(v) for k, v in serialized_args.items()}

    def serialize(self, obj: Any, disable_cache: bool = False) -> str:
        """Serialize an object, storing externally if it meets size thresholds.

        Returns either an inline serialized string (small values) or a
        reference key pointing to externally stored data (large values).

        :param Any obj: Object to serialize
        :param bool disable_cache: If True, always return inline serialized string
        :return: Serialized string or reference key
        """
        if self.conf.disable_client_data_store or disable_cache:
            return self.app.serializer.serialize(obj)
        if isinstance(obj, str) and self.is_reference(obj):
            return obj
        serialized = self.app.serializer.serialize(obj)
        key = self._maybe_store(serialized)
        if self.is_reference(key):
            self._cache_deserialized(key, obj)
        return key

    def resolve(self, data: str) -> Any:
        """Resolve a serialized value to a Python object.

        If the value is a reference key, loads the data from external
        storage first. Otherwise deserializes directly.

        :param str data: Serialized string or reference key
        :return: The deserialized Python object
        """
        if self.is_reference(data):
            return self._resolve_reference(data)
        return self.app.serializer.deserialize(data)

    def deserialize(self, data: str) -> Any:
        """Alias for ``resolve()`` — resolve a serialized value to a Python object.

        .. deprecated::
            Use ``resolve()`` instead for clarity.

        :param str data: Serialized string or reference key
        :return: The deserialized Python object
        """
        return self.resolve(data)

    def is_reference(self, value: str) -> bool:
        """
        Check if a string is a reference key to externally stored data.

        :param str value: String to check
        :return: True if this is a reference key
        """
        return value.startswith(ReservedKeys.CLIENT_DATA.value)

    def purge(self) -> None:
        """Clear local cache and backend storage."""
        self._deserialized_cache.clear()
        self._purge()

    # ── Internal logic ─────────────────────────────────────────────

    def _maybe_store(self, serialized: str) -> str:
        """
        Route serialized data to external storage or return inline.

        Below min_size_to_cache: return inline.
        Above max_size_to_cache (if set): return inline with warning.
        Otherwise: store externally and return reference key.
        """
        size = len(serialized)
        if size < self.conf.min_size_to_cache:
            return serialized
        if self.conf.max_size_to_cache > 0 and size > self.conf.max_size_to_cache:
            # TODO: if it's too big, it will create chunks and store the sequence to later rebuild the argument
            self._log_size_warning(size, "exceeds max_size_to_cache")
            return serialized
        if size > self.conf.warn_threshold:
            self._log_size_warning(size, "exceeds warn_threshold")
        key = _generate_key(serialized)
        self._store(key, serialized)
        return key

    def _resolve_reference(self, ref_key: str) -> Any:
        """
        Resolve a reference key to the deserialized object.

        Uses a small process-local LRU cache to avoid repeated backend reads
        for the same key within a single process.
        """
        if ref_key in self._deserialized_cache:
            self._deserialized_cache.move_to_end(ref_key)
            return self._deserialized_cache[ref_key]
        serialized = self._retrieve(ref_key)
        obj = self.app.serializer.deserialize(serialized)
        self._cache_deserialized(ref_key, obj)
        return obj

    def _cache_deserialized(self, key: str, obj: Any) -> None:
        """Add to LRU cache, evicting oldest if at capacity."""
        if len(self._deserialized_cache) >= self.conf.local_cache_size:
            self._deserialized_cache.popitem(last=False)
        self._deserialized_cache[key] = obj

    # ── Logging ────────────────────────────────────────────────────

    def _log_size_warning(self, size: int, reason: str) -> None:
        """Log a warning about value size."""
        self._logger.warning(
            f"Value size ({size} chars) {reason}. "
            "Consider restructuring to use smaller values."
        )

    # ── Abstract methods for backend implementations ───────────────

    @abstractmethod
    def _store(self, key: str, value: str) -> None:
        """
        Store a serialized value by its content-hash key.

        Backends should use upsert/INSERT OR REPLACE semantics so that
        storing the same key twice is a no-op (content-hash deduplication).

        :param str key: Content-hash reference key
        :param str value: Serialized string to store
        """

    @abstractmethod
    def _retrieve(self, key: str) -> str:
        """
        Retrieve a serialized value by its reference key.

        :param str key: Content-hash reference key
        :return: The stored serialized string
        :raises KeyError: If key not found
        """

    @abstractmethod
    def _purge(self) -> None:
        """Remove all stored data from the backend."""


# ── Module-level helpers ───────────────────────────────────────────


def _generate_key(value: str) -> str:
    """Generate a content-hash reference key from serialized data."""
    hash_value = hashlib.sha256(value.encode()).hexdigest()
    return f"{ReservedKeys.CLIENT_DATA.value}:{hash_value}"
