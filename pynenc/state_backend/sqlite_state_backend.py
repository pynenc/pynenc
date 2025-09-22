"""
SQLite-based state backend for cross-process testing.

This module provides a SQLite-based state backend implementation that enables
true cross-process coordination for testing process runners. Unlike shared memory,
SQLite provides ACID transactions and handles concurrent access automatically.
"""

from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator, Optional

from pynenc.app_info import AppInfo
from pynenc.conf.config_state_backend import ConfigStateBackendSQLite
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.state_backend.base_state_backend import BaseStateBackend, InvocationHistory
from pynenc.types import Params, Result
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import (
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)
from pynenc.workflow import WorkflowIdentity

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class Tables:
    RESULTS = "state_backend_results"
    EXCEPTIONS = "state_backend_exceptions"
    INVOCATIONS = "state_backend_invocations"
    HISTORY = "state_backend_history"
    WORKFLOWS = "state_backend_workflows"
    APP_INFO = "state_backend_app_info"
    WORKFLOW_DATA = "state_backend_workflow_data"
    WORKFLOW_SUB_INVOCATIONS = "state_backend_workflow_sub_invocations"


def init_tables(sqlite_db_path: str) -> None:
    """Initialize SQLite tables for state backend."""
    with sqlite_conn(sqlite_db_path) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.RESULTS} (
                invocation_id TEXT PRIMARY KEY,
                result_data BLOB NOT NULL
            )
        """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.EXCEPTIONS} (
                invocation_id TEXT PRIMARY KEY,
                exception_data BLOB NOT NULL
            )
        """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.INVOCATIONS} (
                invocation_id TEXT PRIMARY KEY,
                invocation_json TEXT NOT NULL
            )
        """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.HISTORY} (
                invocation_id TEXT NOT NULL,
                history_timestamp REAL NOT NULL,
                history_status TEXT NOT NULL,
                history_json TEXT NOT NULL,
                PRIMARY KEY (invocation_id, history_timestamp, history_status)
            )
        """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.WORKFLOWS} (
                workflow_id TEXT PRIMARY KEY,
                workflow_type TEXT NOT NULL,
                workflow_json TEXT NOT NULL
            )
        """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_state_backend_workflows_type ON {Tables.WORKFLOWS}(workflow_type)"
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.APP_INFO} (
                app_id TEXT PRIMARY KEY,
                app_info_json TEXT NOT NULL
            )
        """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.WORKFLOW_DATA} (
                workflow_id TEXT NOT NULL,
                data_key TEXT NOT NULL,
                data_value BLOB NOT NULL,
                PRIMARY KEY (workflow_id, data_key)
            )
        """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.WORKFLOW_SUB_INVOCATIONS} (
                parent_workflow_id TEXT NOT NULL,
                sub_invocation_id TEXT NOT NULL,
                PRIMARY KEY (parent_workflow_id, sub_invocation_id)
            )
        """
        )
        conn.commit()


class SQLiteStateBackend(BaseStateBackend[Params, Result]):
    """
    A SQLite-based implementation of the state backend for cross-process testing.

    Stores invocation data, history, results, and exceptions in SQLite database
    which allows state sharing between processes and is suitable for testing
    process runners.

    ```{warning}
    The `SQLiteStateBackend` class is designed for testing purposes only and should
    not be used in production systems. It uses temporary SQLite files for state.
    ```
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)

        # Use database path from configuration with validation
        self.sqlite_db_path = get_sqlite_sqlite_db_path(self.conf.sqlite_db_path)

        # Initialize database tables
        self.app.logger.warning(
            f"Using SQLite database at {self.sqlite_db_path} for state backend."
        )
        init_tables(self.sqlite_db_path)

    @cached_property
    def conf(self) -> ConfigStateBackendSQLite:
        return ConfigStateBackendSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def store_app_info(self, app_info: "AppInfo") -> None:
        """Store app info"""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.APP_INFO} (app_id, app_info_json) VALUES (?, ?)",
                (app_info.app_id, app_info.to_json()),
            )
            conn.commit()

    def get_app_info(self) -> "AppInfo":
        """Retrieve app info for the current app"""
        from pynenc.app import AppInfo

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT app_info_json FROM {Tables.APP_INFO} WHERE app_id = ?",
                (self.app.app_id,),
            )
            row = cursor.fetchone()
            if row:
                return AppInfo.from_json(row[0])
            raise KeyError(f"App info for {self.app.app_id} not found")

    @staticmethod
    def discover_app_infos() -> dict[str, "AppInfo"]:
        """Retrieve all app information registered in this state backend."""
        default_conf = ConfigStateBackendSQLite()
        sqlite_db_path = get_sqlite_sqlite_db_path(default_conf.sqlite_db_path)
        apps = {}
        with sqlite_conn(sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT app_id, app_info_json FROM {Tables.APP_INFO}"
            )
            for row in cursor.fetchall():
                apps[row[0]] = AppInfo.from_json(row[1])
        return apps

    def store_workflow_run(self, workflow_identity: "WorkflowIdentity") -> None:
        """Store a workflow run for tracking and monitoring."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            w_id = workflow_identity
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.WORKFLOWS} (workflow_id, workflow_type, workflow_json) VALUES (?, ?, ?)",
                (w_id.workflow_id, w_id.workflow_type, w_id.to_json()),
            )
            conn.commit()

    def _upsert_invocations(self, invocations: list["DistributedInvocation"]) -> None:
        """Updates or inserts multiple invocations."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            for invocation in invocations:
                conn.execute(
                    f"INSERT OR REPLACE INTO {Tables.INVOCATIONS} (invocation_id, invocation_json) VALUES (?, ?)",
                    (invocation.invocation_id, invocation.to_json()),
                )
            conn.commit()

    def _get_invocation(self, invocation_id: str) -> Optional["DistributedInvocation"]:
        """Retrieves an invocation by its ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_json FROM {Tables.INVOCATIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row:
                return DistributedInvocation.from_json(self.app, row[0])
        return None

    def _add_histories(
        self, invocation_ids: list[str], invocation_history: "InvocationHistory"
    ) -> None:
        """Adds the same history record for a list of invocations.

        Stores history entries with explicit timestamp and status so that multiple
        status updates per invocation can be preserved and uniquely identified.
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            # Use the timestamp and status attributes from InvocationHistory as part of the PK
            history_ts = invocation_history._timestamp
            history_status = invocation_history.status
            for invocation_id in invocation_ids:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {Tables.HISTORY}
                    (invocation_id, history_timestamp, history_status, history_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        invocation_id,
                        history_ts,
                        history_status,
                        invocation_history.to_json(),
                    ),
                )
            conn.commit()

    def _get_history(self, invocation_id: str) -> list["InvocationHistory"]:
        """Retrieves the history of an invocation ordered by timestamp."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""
                SELECT history_json FROM {Tables.HISTORY}
                WHERE invocation_id = ?
                ORDER BY history_timestamp ASC
                """,
                (invocation_id,),
            )
            rows = cursor.fetchall()
            cursor.close()
            return [InvocationHistory.from_json(r[0]) for r in rows]

    def _get_result(self, invocation_id: str) -> Result:
        """Retrieves the result of an invocation by ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT result_data FROM {Tables.RESULTS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            if row:
                return self.app.serializer.deserialize(row[0])
            raise KeyError(f"Result for invocation {invocation_id} not found")

    def _set_result(self, invocation_id: str, result: Result) -> None:
        """Sets the result of an invocation by ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.RESULTS} (invocation_id, result_data) VALUES (?, ?)",
                (invocation_id, self.app.serializer.serialize(result)),
            )
            conn.commit()

    def _get_exception(self, invocation_id: str) -> Exception:
        """
        Retrieves the exception of an invocation by ID.

        :param str invocation_id: The ID of the invocation
        :return: The exception object
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT exception_data FROM {Tables.EXCEPTIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row:
                return self.deserialize_exception(row[0])
            raise KeyError(f"Exception for invocation {invocation_id} not found")

    def _set_exception(self, invocation_id: str, exception: Exception) -> None:
        """Sets the raised exception by invocation ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.EXCEPTIONS} (invocation_id, exception_data) VALUES (?, ?)",
                (invocation_id, self.serialize_exception(exception)),
            )
            conn.commit()

    # Workflow-related methods
    def set_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """Set workflow data."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            serialized_value = self.app.serializer.serialize(value)
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {Tables.WORKFLOW_DATA} (workflow_id, data_key, data_value)
                VALUES (?, ?, ?)
            """,
                (workflow_identity.workflow_invocation_id, key, serialized_value),
            )
            conn.commit()

    def get_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, default: Any = None
    ) -> Any:
        """Get workflow data."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""
                SELECT data_value FROM {Tables.WORKFLOW_DATA}
                WHERE workflow_id = ? AND data_key = ?
            """,
                (workflow_identity.workflow_invocation_id, key),
            )
            row = cursor.fetchone()
            cursor.close()
            if row:
                return self.app.serializer.deserialize(row[0])
            return default

    def get_all_workflows(self) -> Iterator[str]:
        """Retrieve all workflow IDs."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT DISTINCT workflow_id FROM {Tables.WORKFLOWS}"
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for row in cursor_rows:
                yield row[0]

    def get_all_workflows_runs(self) -> Iterator["WorkflowIdentity"]:
        """Retrieve all stored workflows."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(f"SELECT workflow_json FROM {Tables.WORKFLOWS}")
            cursor_rows = cursor.fetchall()
            cursor.close()
            for row in cursor_rows:
                yield WorkflowIdentity.from_json(row[0])

    def get_workflow_runs(self, workflow_type: str) -> Iterator["WorkflowIdentity"]:
        """Retrieve workflow runs for a specific task."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT workflow_json FROM {Tables.WORKFLOWS} WHERE workflow_type = ?",
                (workflow_type,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for row in cursor_rows:
                yield WorkflowIdentity.from_json(row[0])

    def store_workflow_sub_invocation(
        self, parent_workflow_id: str, sub_invocation_id: str
    ) -> None:
        """Store workflow sub-invocation relationship."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {Tables.WORKFLOW_SUB_INVOCATIONS} (parent_workflow_id, sub_invocation_id)
                VALUES (?, ?)
            """,
                (parent_workflow_id, sub_invocation_id),
            )
            conn.commit()

    def get_workflow_sub_invocations(self, workflow_id: str) -> Iterator[str]:
        """Get workflow sub-invocations."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""
                SELECT sub_invocation_id FROM {Tables.WORKFLOW_SUB_INVOCATIONS}
                WHERE parent_workflow_id = ?
            """,
                (workflow_id,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for row in cursor_rows:
                yield row[0]

    def purge(self) -> None:
        """Clear all state backend data"""
        delete_tables_with_prefix(self.sqlite_db_path, "state_backend")
        init_tables(self.sqlite_db_path)
