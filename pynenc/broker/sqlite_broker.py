"""
SQLite-based broker for cross-process testing.

This module provides a SQLite-based broker implementation that enables
true cross-process coordination for testing process runners.
"""

from functools import cached_property
from typing import TYPE_CHECKING

from pynenc.broker.base_broker import BaseBroker
from pynenc.conf.config_broker import ConfigBrokerSQLite
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import (
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation


class Tables:
    QUEUE = "broker_message_queue"


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

        # Use database path from configuration with validation
        self.sqlite_db_path = get_sqlite_sqlite_db_path(self.conf.sqlite_db_path)

        # Initialize database tables
        self._init_tables()

    def _init_tables(self) -> None:
        """Initialize SQLite tables for broker."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.QUEUE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invocation_id TEXT NOT NULL,
                    invocation_json TEXT NOT NULL,
                    created_at REAL NOT NULL DEFAULT (julianday('now'))
                )
            """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_created_at ON {Tables.QUEUE}(created_at)
            """
            )
            conn.commit()

    @cached_property
    def conf(self) -> ConfigBrokerSQLite:
        return ConfigBrokerSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def send_message(self, invocation: "DistributedInvocation") -> None:
        """Send a message (invocation) to the queue."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT INTO {Tables.QUEUE} (invocation_id, invocation_json, created_at) VALUES (?, ?, julianday('now'))",
                (invocation.invocation_id, invocation.to_json()),
            )
            conn.commit()

    def route_invocation(self, invocation: "DistributedInvocation") -> None:
        """Route a single invocation by sending it to the message queue."""
        self.send_message(invocation)

    def route_invocations(self, invocations: list["DistributedInvocation"]) -> None:
        """Route multiple invocations by sending them to the message queue."""
        for invocation in invocations:
            self.route_invocation(invocation)

    def retrieve_invocation(self) -> "DistributedInvocation | None":
        """
        Atomically retrieve and remove a single invocation from the queue.
        Ensures that no two processes can retrieve the same invocation.
        :return: The next DistributedInvocation in the queue, or None if empty.
        """
        from pynenc.invocation.dist_invocation import DistributedInvocation

        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")  # Lock for atomicity
            cursor = conn.execute(
                f"SELECT id, invocation_json FROM {Tables.QUEUE} ORDER BY created_at ASC LIMIT 1"
            )
            row = cursor.fetchone()
            cursor.close()
            if not row:
                return None
            message_id, invocation_json = row
            conn.execute(f"DELETE FROM {Tables.QUEUE} WHERE id = ?", (message_id,))
            conn.commit()
            return DistributedInvocation.from_json(self.app, invocation_json)

    def count_invocations(self) -> int:
        """Count the number of invocations in the queue."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {Tables.QUEUE}")
            return cursor.fetchone()[0]

    def purge(self) -> None:
        """Clear all broker messages."""
        delete_tables_with_prefix(self.sqlite_db_path, "broker_")
        self._init_tables()
