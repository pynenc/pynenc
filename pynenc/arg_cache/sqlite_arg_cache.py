"""
SQLite-based argument cache for cross-process testing.

This module provides a SQLite-based argument cache implementation that enables
true cross-process caching for testing process runners.
"""

import sqlite3
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pynenc.arg_cache.base_arg_cache import BaseArgCache
from pynenc.conf.config_arg_cache import ConfigArgCacheSQLite
from pynenc.util.sqlite_utils import (
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class Tables:
    CACHE = "arg_cache"


class SQLiteArgCache(BaseArgCache):
    """
    A SQLite-based implementation of the argument cache for cross-process testing.

    Uses SQLite for cross-process argument caching and implements
    all required abstract methods from BaseArgCache. It's designed specifically
    for testing process runners.

    ```{warning}
    The `SQLiteArgCache` class is designed for testing purposes only and should
    not be used in production systems. It uses temporary SQLite files for state.
    ```
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)

        # Use database path from configuration with validation
        self.sqlite_db_path = get_sqlite_sqlite_db_path(self.conf.sqlite_db_path)

        # Initialize database tables
        self._init_tables()

    def _init_tables(self) -> None:
        """Initialize SQLite tables for argument cache."""
        with sqlite3.connect(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.CACHE} (
                    cache_key TEXT PRIMARY KEY,
                    cached_data BLOB NOT NULL,
                    created_at REAL NOT NULL DEFAULT (julianday('now'))
                )
            """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_created_at ON {Tables.CACHE}(created_at)"
            )
            conn.commit()

    @cached_property
    def conf(self) -> ConfigArgCacheSQLite:
        return ConfigArgCacheSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def store(self, cache_key: str, value: Any) -> None:
        """Store a value in the cache."""
        import pickle

        with sqlite3.connect(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.CACHE} (cache_key, cached_data, created_at) VALUES (?, ?, julianday('now'))",
                (cache_key, pickle.dumps(value)),
            )
            conn.commit()

    def retrieve(self, cache_key: str) -> Any:
        """Retrieve a value from the cache."""
        import pickle

        with sqlite3.connect(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT cached_data FROM {Tables.CACHE} WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()
            if row:
                return pickle.loads(row[0])
            raise KeyError(f"Cache key {cache_key} not found")

    def exists(self, cache_key: str) -> bool:
        """Check if a cache key exists."""
        with sqlite3.connect(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT 1 FROM {Tables.CACHE} WHERE cache_key = ?", (cache_key,)
            )
            return cursor.fetchone() is not None

    def _store(self, key: str, value: str) -> None:
        """
        Store a key value pair in the cache.

        :param str key: The cache key
        :param str value: The string value to cache
        """
        with sqlite3.connect(self.sqlite_db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO arg_cache (cache_key, cached_data) VALUES (?, ?)",
                (key, value.encode("utf-8")),
            )
            conn.commit()

    def _retrieve(self, key: str) -> str:
        """
        Retrieve a serialized value from the cache by its key.

        :param str key: The cache key
        :return: The cached serialized value
        :raises KeyError: If the key is not found
        """
        with sqlite3.connect(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                "SELECT cached_data FROM arg_cache WHERE cache_key = ?", (key,)
            )
            row = cursor.fetchone()
            if row:
                return row[0].decode("utf-8")
            raise KeyError(f"Cache key {key} not found")

    def _purge(self) -> None:
        """Clear all cached data."""
        delete_tables_with_prefix(self.sqlite_db_path, "arg")
        self._init_tables()
