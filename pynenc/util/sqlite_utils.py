"""
SQLite utilities for Pynenc shared state management.

Provides small helpers for creating connections and simple cross-process
operations used by the test orchestrator/state components.
"""

import hashlib
import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class SQLiteConnection:
    """
    A wrapper for sqlite3.Connection that adds retry logic to execute method.

    This wrapper delegates all methods to the underlying connection, but overrides
    execute to include exponential backoff retry on database lock errors.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(
        self,
        sql: str,
        parameters: tuple[Any, ...] = (),
        /,
    ) -> sqlite3.Cursor:
        """
        Execute a SQL query with exponential backoff retry on database lock errors.

        :param sql: SQL query string
        :param parameters: Query parameters
        :return: Cursor from the successful execution
        """
        max_retries = 5
        initial_backoff = 0.1
        backoff = initial_backoff
        for attempt in range(max_retries + 1):
            try:
                return self._conn.execute(sql, parameters)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries:
                    logger.warning(
                        f"Database locked, retrying in {backoff}s (attempt {attempt + 1})"
                    )
                    time.sleep(backoff)
                    backoff *= 2
                else:
                    logger.error("SQLite operation failed: %s", e)
                    raise
        raise RuntimeError("Max retries exceeded for database operation")

    def __getattr__(self, name: str) -> Any:
        """Delegate all other attributes to the underlying connection."""
        return getattr(self._conn, name)

    def __enter__(self) -> "SQLiteConnection":
        """Enter context manager."""
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type: type | None, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager."""
        self._conn.__exit__(exc_type, exc_val, exc_tb)


def create_sqlite_connection(sqlite_db_path: str | Path) -> SQLiteConnection:
    """
    Create and return a configured sqlite3.Connection for concurrent test use.

    The connection uses WAL journal mode and a busy timeout so concurrent
    clients are less likely to raise transient "database is locked" errors.

    :param sqlite_db_path: Path to the SQLite database file
    :return: A configured sqlite3.Connection
    """
    conn = sqlite3.connect(str(sqlite_db_path), timeout=30.0, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA busy_timeout=30000")
    except sqlite3.DatabaseError as e:
        # Non-fatal: some environments may not accept all pragmas
        logger.warning("PRAGMA configuration failed: %s", e)

    return SQLiteConnection(conn)


def get_sqlite_sqlite_db_path(sqlite_db_path: str) -> str:
    """
    Get and validate the SQLite database path.

    :param sqlite_db_path: The configured database path
    :return: The validated database path
    :raises ValueError: If no database path is configured
    """
    if not sqlite_db_path:
        raise ValueError("SQLite database path must be configured.")

    sqlite_db_path_obj = Path(sqlite_db_path)
    sqlite_db_path_obj.parent.mkdir(parents=True, exist_ok=True)
    return str(sqlite_db_path_obj)


def sanitize_table_prefix(app_id: str) -> str:
    """
    Sanitize an app_id for safe use as a SQLite table name prefix.

    Replaces any character that is not alphanumeric or underscore with an
    underscore, prepends an underscore if the result starts with a digit,
    and always appends an 8-character hash of the original app_id.

    The hash prevents collisions when two different app_ids sanitize to the
    same string (e.g. ``my-app`` and ``my_app``), and also protects against
    a user accidentally choosing an app_id that looks like a previously
    sanitized value.

    :param str app_id: The application identifier to sanitize
    :return: A string safe for use in SQLite table names
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", app_id)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    sanitized = sanitized or "_default"
    hash_suffix = hashlib.sha256(app_id.encode()).hexdigest()[:8]
    return f"{sanitized}_{hash_suffix}"


class TableNames:
    """Base class for app-scoped SQLite table name collections.

    Subclasses call ``super().__init__(app_id, component)`` where *component*
    is a short label such as ``"broker"`` or ``"state_backend"``.  The resulting
    :attr:`table_prefix` is the single source of truth used both when creating
    tables and when purging them, eliminating duplicated prefix strings.
    """

    def __init__(self, app_id: str, component: str) -> None:
        self.table_prefix: str = f"{sanitize_table_prefix(app_id)}__{component}"


def delete_tables_with_prefix(sqlite_db_path: str | Path, prefix: str) -> None:
    """
    Delete all data from tables in the SQLite database that start with the given prefix.

    Uses a short-lived connection and closes cursors promptly to avoid holding
    read cursors open which can cause 'database is locked' when other clients write.

    :param sqlite_db_path: Path to the SQLite database file
    :param prefix: Table name prefix to match
    """
    with create_sqlite_connection(sqlite_db_path) as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (f"{prefix}%",),
        )
        try:
            tables = [row[0] for row in cursor.fetchall()]
        finally:
            try:
                cursor.close()
            except Exception:
                pass

        for table in tables:
            conn.execute(f"DELETE FROM {table}")
        conn.commit()
