import hashlib
from abc import ABC, abstractmethod
from enum import StrEnum, auto
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pynenc.conf.config_arg_cache import ConfigArgCache
from pynenc.serializer.constants import ReservedKeys

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class ArgCacheKeys(StrEnum):
    """Keys used in runner cache."""

    ARG_CACHE_ID = auto()
    ARG_CACHE_HASH = auto()
    ARG_CACHE_FINGERPRINT = auto()
    ARG_CACHE_KEY = auto()
    ARG_CACHE_DESERIALIZED = auto()


class BaseArgCache(ABC):
    """
    Base class for argument caching system.

    Caches large serialized arguments to avoid repeatedly sending them between tasks.
    Subclasses should implement the actual storage mechanism.
    """

    def __init__(self, app: "Pynenc") -> None:
        """Initialize with app reference."""
        self.app = app
        # Initialize local caches for fallback
        self._local_caches: dict = {}
        self._using_local = False
        self._warned_local = False
        # Initialize caches (either local or runner)
        self._initialize_caches()

    def _initialize_caches(self) -> None:
        """
        Initialize cache sections if they don't exist.
        Safe to call multiple times - won't overwrite existing data.
        Works for both local and runner caches.
        """
        try:
            # Try to use runner cache
            cache = self.app.runner.cache
            self._using_local = False
        except (AttributeError, KeyError):
            # Fallback to local cache
            cache = self._local_caches
            self._using_local = True
            if not self._warned_local:
                self.app.logger.debug(
                    "Runner cache not initialized. Using local cache. "
                    "This is expected if running outside a Pynenc runner context."
                )
                self._warned_local = True

        # Initialize sections only if they don't exist
        for key in ArgCacheKeys:
            if key.value not in cache:
                cache[key.value] = {}

    def _get_cache(self, key: ArgCacheKeys) -> dict:
        """
        Get the appropriate cache dictionary.
        Ensures caches are initialized.

        :param ArgCacheKeys key: The cache section to access
        :return: The cache dictionary to use
        """
        try:
            return self.app.runner.cache[key.value]
        except (AttributeError, KeyError):
            if key.value not in self._local_caches:
                self._local_caches[key.value] = {}
            return self._local_caches[key.value]

    @property
    def _obj_id_cache(self) -> dict[int, str]:
        """Object ID cache with fallback."""
        return self._get_cache(ArgCacheKeys.ARG_CACHE_ID)

    @property
    def _hash_cache(self) -> dict[int, str]:
        """Hash cache with fallback."""
        return self._get_cache(ArgCacheKeys.ARG_CACHE_HASH)

    @property
    def _fingerprint_cache(self) -> dict[tuple[str, int], str]:
        """Fingerprint cache with fallback."""
        return self._get_cache(ArgCacheKeys.ARG_CACHE_FINGERPRINT)

    @property
    def _key_cache(self) -> dict[str, str]:
        """Key cache with fallback."""
        return self._get_cache(ArgCacheKeys.ARG_CACHE_KEY)

    @property
    def _deserialized_cache(self) -> dict[str, Any]:
        """Deserialized object cache with fallback."""
        return self._get_cache(ArgCacheKeys.ARG_CACHE_DESERIALIZED)

    @cached_property
    def conf(self) -> ConfigArgCache:
        """Get the argument cache configuration."""
        return ConfigArgCache(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def _check_object_identity(self, obj: Any) -> str | None:
        """Check if object is already cached by identity."""
        obj_id = id(obj)
        return self._obj_id_cache.get(obj_id)

    def _check_object_hash(self, obj: Any) -> tuple[str | None, int]:
        """
        Try to get cached value using object hash.
        Returns (cached_key, obj_id) or (None, obj_id).
        """
        obj_id = id(obj)
        try:
            if key := self._hash_cache.get(hash(obj)):
                self._update_obj_id_cache(obj_id, key)
                return key, obj_id
        except TypeError:
            pass  # Not hashable
        return None, obj_id

    def _check_fingerprint(self, serialized: str, obj_id: int, obj: Any) -> str | None:
        """Check if serialized content matches by fingerprint."""
        fingerprint = (serialized[:128], len(serialized))
        if key := self._fingerprint_cache.get(fingerprint):
            self._update_obj_id_cache(obj_id, key)
            self._update_hash_cache(obj, key)
            return key
        return None

    def _check_exact_match(
        self, serialized: str, obj_id: int, obj: Any
    ) -> tuple[str | None, tuple[str, int]]:
        """
        Check if exact serialized content is cached.
        Returns (cached_key, fingerprint) or (None, fingerprint).
        """
        fingerprint = (serialized[:128], len(serialized))
        if key := self._key_cache.get(serialized):
            self._update_obj_id_cache(obj_id, key)
            self._update_hash_cache(obj, key)
            self._update_fingerprint_cache(fingerprint, key)
            return key, fingerprint
        return None, fingerprint

    def _store_new_value(
        self,
        serialized: str,
        obj_id: int,
        obj: Any,
        fingerprint: tuple[str, int],
    ) -> str:
        """Store new value and update all caches."""
        key = self._generate_key(serialized)
        self._store(key, serialized)

        # Update all caches
        self._update_obj_id_cache(obj_id, key)
        self._update_hash_cache(obj, key)
        self._update_fingerprint_cache(fingerprint, key)
        self._update_key_cache(serialized, key)
        self._update_deserialized_cache(key, obj)

        return key

    def serialize(self, obj: Any, disable_cache: bool = False) -> str:
        """
        Serialize an object, potentially caching if it meets size requirements.

        :param Any obj: Object to serialize
        :param bool disable_cache: If True, skips caching regardless of size
        :return: Either the serialized string or a cache key
        """
        if disable_cache:
            return self.app.serializer.serialize(obj)

        # Try object identity cache (fastest)
        if key := self._check_object_identity(obj):
            return key

        # Try object hash cache
        key, obj_id = self._check_object_hash(obj)
        if key:
            return key

        # Serialize the object
        serialized = self.app.serializer.serialize(obj)

        # Check if large enough to cache
        if len(serialized) < self.conf.min_size_to_cache:
            return serialized

        # Try fingerprint cache
        if key := self._check_fingerprint(serialized, obj_id, obj):
            return key

        # Try exact match cache
        key, fingerprint = self._check_exact_match(serialized, obj_id, obj)
        if key:
            return key

        # Store new value and update caches
        return self._store_new_value(serialized, obj_id, obj, fingerprint)

    def deserialize(self, data: str) -> Any:
        """
        Deserialize data, checking if it's a cache key first.

        :param str data: Data to deserialize (might be a cache key)
        :return: The deserialized object
        """
        if not self.is_cache_key(data):
            return self.app.serializer.deserialize(data)
        cache_key = data
        # Check deserialized cache first
        if deserialized_data := self._deserialized_cache.get(cache_key):
            return deserialized_data
        # Get from storage and deserialize
        data = self._retrieve(cache_key)
        obj = self.app.serializer.deserialize(data)
        # Update all caches for future lookups
        self._update_obj_id_cache(id(obj), cache_key)
        self._update_hash_cache(obj, cache_key)
        self._update_deserialized_cache(cache_key, obj)
        return obj

    def is_cache_key(self, value: str) -> bool:
        """Check if a string is a cache key."""
        return value.startswith(ReservedKeys.ARG_CACHE.value)

    def _generate_key(self, value: str) -> str:
        """Generate a cache key for a value."""
        hash_value = hashlib.sha256(value.encode()).hexdigest()
        return f"{ReservedKeys.ARG_CACHE.value}:{hash_value}"

    def _update_obj_id_cache(self, obj_id: int, key: str) -> None:
        """Update the object ID cache with LRU eviction."""
        if len(self._obj_id_cache) >= self.conf.local_cache_size:
            self._obj_id_cache.pop(next(iter(self._obj_id_cache)))
        self._obj_id_cache[obj_id] = key

    def _update_hash_cache(self, obj: Any, key: str) -> None:
        """Update the hash cache with LRU eviction if object is hashable."""
        try:
            obj_hash = hash(obj)
            if len(self._hash_cache) >= self.conf.local_cache_size:
                self._hash_cache.pop(next(iter(self._hash_cache)))
            self._hash_cache[obj_hash] = key
        except TypeError:
            pass  # Not hashable, skip

    def _update_fingerprint_cache(self, fingerprint: tuple[str, int], key: str) -> None:
        """Update the fingerprint cache with LRU eviction."""
        if len(self._fingerprint_cache) >= self.conf.local_cache_size:
            self._fingerprint_cache.pop(next(iter(self._fingerprint_cache)))
        self._fingerprint_cache[fingerprint] = key

    def _update_key_cache(self, serialized: str, key: str) -> None:
        """Update the key cache with LRU eviction."""
        if len(self._key_cache) >= self.conf.local_cache_size:
            self._key_cache.pop(next(iter(self._key_cache)))
        self._key_cache[serialized] = key

    def _update_deserialized_cache(self, key: str, obj: Any) -> None:
        """Update the value cache with LRU eviction."""
        if len(self._deserialized_cache) >= self.conf.local_cache_size:
            self._deserialized_cache.pop(next(iter(self._deserialized_cache)))
        self._deserialized_cache[key] = obj

    def purge(self) -> None:
        """
        Clear all cached arguments.

        Should clear both the in-memory caches and the storage backend.
        Handles cases where the runner cache is a Manager.dict() that might be shut down.
        """
        try:
            # Clear all sections in the cache (either runner or local)
            for key in ArgCacheKeys:
                try:
                    cache = self._get_cache(key)
                    if isinstance(
                        cache, dict
                    ):  # Check if it's a valid dict-like object
                        cache.clear()
                except (AttributeError, KeyError):
                    self.app.logger.debug(f"Nothing to purge on {key=}")
                except (BrokenPipeError, EOFError, ValueError):
                    # Handle cases where Manager.dict() is closed or inaccessible
                    self.app.logger.debug(
                        f"Could not clear runner cache section {key} - manager might be shut down"
                    )
        except Exception as e:
            self.app.logger.warning(f"Unexpected error clearing runner cache: {e}")
        finally:
            # Clear backend storage
            try:
                self._purge()
            except Exception as e:
                self.app.logger.warning(f"Failed to purge backend storage: {e}")

    @abstractmethod
    def _store(self, key: str, value: str) -> None:
        """
        Store a key value pair in the cache

        :param str key: The cache key
        :param str value: The string value to cache
        :return: Cache key for the stored value
        """

    @abstractmethod
    def _retrieve(self, key: str) -> str:
        """
        Retrieve a serialized value from the cache by its key.

        :param str key: The cache key
        :return: The cached serialized value
        :raises KeyError: If the key is not found
        """

    @abstractmethod
    def _purge(self) -> None:
        """
        Clear all cached arguments.
        """
