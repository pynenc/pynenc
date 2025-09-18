"""
SQLite utilities for Pynenc shared state management.

Provides small helpers for creating connections and simple cross-process
operations used by the test orchestrator/state components.
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)


def create_sqlite_connection(sqlite_db_path: str | Path) -> sqlite3.Connection:
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
    return conn


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
