"""
SQLite-based broker for cross-process testing.

This module provides a SQLite-based broker implementation that enables
true cross-process coordination for testing process runners.
"""

from functools import cached_property
from typing import TYPE_CHECKING

from pynenc.broker.base_broker import BaseBroker
from pynenc.conf.config_broker import ConfigBrokerSQLite
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.util.sqlite_utils import TableNames
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import (
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class Tables(TableNames):
    """Table names for broker, scoped by app_id."""

    def __init__(self, app_id: str) -> None:
        super().__init__(app_id, "broker")
        self.QUEUE = f"{self.table_prefix}_message_queue"


class SQLiteBroker(BaseBroker):
    """
    A SQLite-based implementation of the broker for cross-process testing.

    Uses SQLite for cross-process message queue coordination and implements
    all required abstract methods from BaseBroker. It's designed specifically
    for testing process runners.

    ```{warning}
    The `SQLiteBroker` class is designed for testing purposes only and should
    not be used in production systems. It uses temporary SQLite files for state.
    ```
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.tables = Tables(app.app_id)

        # Use database path from configuration with validation
        self.sqlite_db_path = get_sqlite_sqlite_db_path(self.conf.sqlite_db_path)

        # Initialize database tables
        self._init_tables()

    def _init_tables(self) -> None:
        """Initialize SQLite tables for broker."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.QUEUE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invocation_id TEXT NOT NULL,
                    created_at REAL NOT NULL DEFAULT (julianday('now'))
                )
            """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.tables.QUEUE}_created_at ON {self.tables.QUEUE}(created_at)
            """
            )
            conn.commit()

    @cached_property
    def conf(self) -> ConfigBrokerSQLite:
        return ConfigBrokerSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def send_message(self, invocation_id: "InvocationId") -> None:
        """Send a message (invocation ID) to the queue."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT INTO {self.tables.QUEUE} (invocation_id, created_at) VALUES (?, julianday('now'))",
                (invocation_id,),
            )
            conn.commit()

    def route_invocation(self, invocation_id: "InvocationId") -> None:
        """Route a single invocation ID by sending it to the message queue."""
        self.send_message(invocation_id)

    def route_invocations(self, invocation_ids: list["InvocationId"]) -> None:
        """Route multiple invocation IDs by sending them to the message queue."""
        for invocation_id in invocation_ids:
            self.route_invocation(invocation_id)

    def retrieve_invocation(self) -> "InvocationId | None":
        """
        Atomically retrieve and remove a single invocation from the queue.
        Ensures that no two processes can retrieve the same invocation.
        :return: The next invocation ID in the queue, or None if empty.
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")  # Lock for atomicity
            cursor = conn.execute(
                f"SELECT id, invocation_id FROM {self.tables.QUEUE} ORDER BY created_at ASC LIMIT 1"
            )
            row = cursor.fetchone()
            cursor.close()
            if not row:
                return None
            message_id, invocation_id = row
            conn.execute(f"DELETE FROM {self.tables.QUEUE} WHERE id = ?", (message_id,))
            conn.commit()
            return InvocationId(invocation_id)

    def count_invocations(self) -> int:
        """Count the number of invocations in the queue."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {self.tables.QUEUE}")
            return cursor.fetchone()[0]

    def purge(self) -> None:
        """Clear all broker messages."""
        delete_tables_with_prefix(self.sqlite_db_path, self.tables.table_prefix)
        self._init_tables()
