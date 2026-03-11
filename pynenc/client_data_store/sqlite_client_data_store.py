"""
SQLite-based client data store for cross-process testing.

Provides a SQLite-backed implementation that enables true cross-process
data sharing for testing process runners.

Key components:
- SQLiteClientDataStore: SQLite-backed client data store

.. warning::
    Designed for testing purposes only. Not recommended for production.
"""

import sqlite3
from functools import cached_property
from typing import TYPE_CHECKING

from pynenc.client_data_store.base_client_data_store import BaseClientDataStore
from pynenc.conf.config_client_data_store import ConfigClientDataStoreSQLite
from pynenc.util.sqlite_utils import (
    TableNames,
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class Tables(TableNames):
    """Table names for client data store, scoped by app_id."""

    def __init__(self, app_id: str) -> None:
        super().__init__(app_id, "client")
        self.STORE = f"{self.table_prefix}_data"


class SQLiteClientDataStore(BaseClientDataStore):
    """
    SQLite-backed client data store for cross-process testing.

    Uses SQLite to allow multiple processes to share cached client data.
    Implements the three abstract methods from BaseClientDataStore.

    .. warning::
        Designed for testing purposes only. Uses temporary SQLite files.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.tables = Tables(app.app_id)
        self.sqlite_db_path = get_sqlite_sqlite_db_path(self.conf.sqlite_db_path)
        self._init_tables()

    def _init_tables(self) -> None:
        """Create the client_data table if it doesn't exist."""
        with sqlite3.connect(self.sqlite_db_path) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables.STORE} (
                    data_key TEXT PRIMARY KEY,
                    data_value BLOB NOT NULL,
                    created_at REAL NOT NULL DEFAULT (julianday('now'))
                )
            """)
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables.STORE}_created "
                f"ON {self.tables.STORE}(created_at)"
            )
            conn.commit()

    @cached_property
    def conf(self) -> ConfigClientDataStoreSQLite:
        """Get SQLite-specific configuration."""
        return ConfigClientDataStoreSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def _store(self, key: str, value: str) -> None:
        """Store a key-value pair in SQLite."""
        with sqlite3.connect(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {self.tables.STORE} "
                f"(data_key, data_value) VALUES (?, ?)",
                (key, value.encode("utf-8")),
            )
            conn.commit()

    def _retrieve(self, key: str) -> str:
        """
        Retrieve a stored value by key.

        :param str key: The reference key
        :return: The stored serialized string
        :raises KeyError: If key not found
        """
        with sqlite3.connect(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT data_value FROM {self.tables.STORE} WHERE data_key = ?",
                (key,),
            )
            if row := cursor.fetchone():
                return row[0].decode("utf-8")
            raise KeyError(f"Key {key} not found in client data store")

    def _purge(self) -> None:
        """Clear all stored client data."""
        delete_tables_with_prefix(self.sqlite_db_path, self.tables.table_prefix)
        self._init_tables()
