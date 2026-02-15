"""
SQLite-based state backend for cross-process testing.

This module provides a SQLite-based state backend implementation that enables
true cross-process coordination for testing process runners. Unlike shared memory,
SQLite provides ACID transactions and handles concurrent access automatically.
"""

from collections.abc import Iterator
from datetime import datetime
from functools import cached_property
import json
from typing import TYPE_CHECKING, Any

from pynenc.app_info import AppInfo
from pynenc.conf.config_state_backend import ConfigStateBackendSQLite
from pynenc.identifiers.call_id import CallId
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.models.call_dto import CallDTO
from pynenc.invocation.dist_invocation import InvocationDTO
from pynenc.runner.runner_context import RunnerContext
from pynenc.state_backend.base_state_backend import BaseStateBackend, InvocationHistory
from pynenc.identifiers.task_id import TaskId
from pynenc.types import Params, Result
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import (
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)
from pynenc.workflow.workflow_identity import WorkflowIdentity

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class Tables:
    RESULTS = "state_backend_results"
    EXCEPTIONS = "state_backend_exceptions"
    INVOCATIONS = "state_backend_invocations"
    RUNNER_CONTEXTS = "state_backend_runner_contexts"
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
                call_id_key TEXT NOT NULL,
                task_id_key TEXT NOT NULL,
                arguments_id TEXT NOT NULL,
                serialized_arguments TEXT NOT NULL,
                parent_invocation_id TEXT,
                parent_call_id TEXT,
                workflow_id TEXT NOT NULL,
                workflow_type_key TEXT NOT NULL,
                parent_workflow_id TEXT
            )
        """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Tables.RUNNER_CONTEXTS} (
                runner_id TEXT PRIMARY KEY,
                runner_cls TEXT NOT NULL,
                parent_ctx_id TEXT,
                pid INTEGER NOT NULL,
                hostname TEXT NOT NULL,
                thread_id INTEGER NOT NULL
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
                workflow_type_key TEXT NOT NULL,
                parent_workflow_id TEXT
            )
        """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_state_backend_workflows_type ON {Tables.WORKFLOWS}(workflow_type_key)"
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
        self.app.logger.debug(
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
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.WORKFLOWS} (workflow_id, workflow_type_key, parent_workflow_id) VALUES (?, ?, ?)",
                (
                    workflow_identity.workflow_id,
                    workflow_identity.workflow_type.key,
                    workflow_identity.parent_workflow_id or "",
                ),
            )
            conn.commit()

    def _upsert_invocations(
        self, entries: list[tuple["InvocationDTO", "CallDTO"]]
    ) -> None:
        """Store invocation and call DTO pairs as discrete columns."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            for inv_dto, call_dto in entries:
                wf = inv_dto.workflow
                conn.execute(
                    f"""INSERT OR REPLACE INTO {Tables.INVOCATIONS}
                    (invocation_id, call_id_key, task_id_key, arguments_id,
                     serialized_arguments,
                     parent_invocation_id,
                     workflow_id, workflow_type_key, parent_workflow_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        inv_dto.invocation_id,
                        call_dto.call_id.key,
                        call_dto.call_id.task_id.key,
                        call_dto.call_id.args_id,
                        json.dumps(call_dto.serialized_arguments),
                        inv_dto.parent_invocation_id,
                        wf.workflow_id,
                        wf.workflow_type.key,
                        wf.parent_workflow_id,
                    ),
                )
            conn.commit()

    def _get_invocation(
        self, invocation_id: str
    ) -> tuple["InvocationDTO", "CallDTO"] | None:
        """Retrieve invocation and call DTOs by invocation ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""SELECT invocation_id, call_id_key, serialized_arguments, parent_invocation_id,
                           workflow_id, workflow_type_key, parent_workflow_id
                    FROM {Tables.INVOCATIONS} WHERE invocation_id = ?""",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row:
                (
                    inv_id,
                    call_id_key,
                    ser_args,
                    parent_inv_id,
                    wf_id,
                    wf_type_key,
                    wf_parent_id,
                ) = row
                call_id = CallId.from_key(call_id_key)
                workflow = WorkflowIdentity(
                    workflow_id=InvocationId(wf_id),
                    workflow_type=TaskId.from_key(wf_type_key),
                    parent_workflow_id=InvocationId(wf_parent_id)
                    if wf_parent_id
                    else None,
                )
                inv_dto = InvocationDTO(
                    invocation_id=InvocationId(inv_id),
                    call_id=call_id,
                    workflow=workflow,
                    parent_invocation_id=InvocationId(parent_inv_id)
                    if parent_inv_id
                    else None,
                )
                call_dto = CallDTO(
                    call_id=call_id,
                    serialized_arguments=json.loads(ser_args),
                )
                return (inv_dto, call_dto)
        return None

    def _add_histories(
        self,
        invocation_ids: list["InvocationId"],
        invocation_history: "InvocationHistory",
    ) -> None:
        """Adds the same history record for a list of invocations.

        Stores history entries with explicit timestamp and status so that multiple
        status updates per invocation can be preserved and uniquely identified.
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            # Use the timestamp and status attributes from InvocationHistory as part of the PK
            # Convert datetime to Unix timestamp for consistent storage and querying
            timestamp_float = invocation_history._timestamp.timestamp()
            for invocation_id in invocation_ids:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {Tables.HISTORY}
                    (invocation_id, history_timestamp, history_status, history_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        invocation_id,
                        timestamp_float,
                        invocation_history.status_record.status,
                        invocation_history.to_json(),
                    ),
                )
            conn.commit()

    def _get_history(self, invocation_id: "InvocationId") -> list["InvocationHistory"]:
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

    def _get_result(self, invocation_id: "InvocationId") -> str:
        """Retrieves the result of an invocation by ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT result_data FROM {Tables.RESULTS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            raise KeyError(f"Result for invocation {invocation_id} not found")

    def _set_result(
        self, invocation_id: "InvocationId", serialized_result: str
    ) -> None:
        """Sets the result of an invocation by ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.RESULTS} (invocation_id, result_data) VALUES (?, ?)",
                (invocation_id, serialized_result),
            )
            conn.commit()

    def _get_exception(self, invocation_id: "InvocationId") -> str:
        """
        Retrieves the exception of an invocation by ID.

        :param InvocationId invocation_id: The ID of the invocation
        :return: The serialized exception string
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT exception_data FROM {Tables.EXCEPTIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row:
                return row[0]
            raise KeyError(f"Exception for invocation {invocation_id} not found")

    def _set_exception(
        self, invocation_id: "InvocationId", serialized_exception: str
    ) -> None:
        """Sets the raised exception by invocation ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.EXCEPTIONS} (invocation_id, exception_data) VALUES (?, ?)",
                (invocation_id, serialized_exception),
            )
            conn.commit()

    # Workflow-related methods
    def set_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """Set workflow data."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            serialized_value = self.app.client_data_store.serialize(value)
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {Tables.WORKFLOW_DATA} (workflow_id, data_key, data_value)
                VALUES (?, ?, ?)
            """,
                (workflow_identity.workflow_id, key, serialized_value),
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
                (workflow_identity.workflow_id, key),
            )
            row = cursor.fetchone()
            cursor.close()
            if row:
                return self.app.client_data_store.deserialize(row[0])
            return default

    def get_all_workflow_types(self) -> Iterator["TaskId"]:
        """Retrieve all workflow IDs."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT DISTINCT workflow_type_key FROM {Tables.WORKFLOWS}"
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for row in cursor_rows:
                yield TaskId.from_key(row[0])

    def get_all_workflow_runs(self) -> Iterator["WorkflowIdentity"]:
        """Retrieve all stored workflows."""
        yield from self._get_workflow_runs(workflow_type_key=None)

    def get_workflow_runs(
        self, workflow_type: "TaskId"
    ) -> Iterator["WorkflowIdentity"]:
        yield from self._get_workflow_runs(workflow_type_key=workflow_type.key)

    def _get_workflow_runs(
        self, workflow_type_key: str | None
    ) -> Iterator["WorkflowIdentity"]:
        """Retrieve workflow runs for a specific task."""
        filter = "WHERE workflow_type_key = ?" if workflow_type_key else ""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT workflow_id, workflow_type_key, parent_workflow_id FROM {Tables.WORKFLOWS} {filter}",
                (workflow_type_key,) if workflow_type_key else (),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for wf_id, wf_type_key, parent_wf_id in cursor_rows:
                yield WorkflowIdentity(
                    workflow_id=InvocationId(wf_id),
                    workflow_type=TaskId.from_key(wf_type_key),
                    parent_workflow_id=InvocationId(parent_wf_id)
                    if parent_wf_id
                    else None,
                )

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

    def get_workflow_sub_invocations(
        self, workflow_id: "InvocationId"
    ) -> Iterator["InvocationId"]:
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
            for (sub_invocation_id,) in cursor_rows:
                yield InvocationId(sub_invocation_id)

    def iter_invocations_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100,
    ) -> Iterator[list["InvocationId"]]:
        """Iterate over invocation IDs that have history within time range."""
        offset = 0
        while True:
            with sqlite_conn(self.sqlite_db_path) as conn:
                cursor = conn.execute(
                    f"""
                    SELECT DISTINCT invocation_id
                    FROM {Tables.HISTORY}
                    WHERE history_timestamp >= ? AND history_timestamp <= ?
                    ORDER BY invocation_id
                    LIMIT ? OFFSET ?
                """,
                    (start_time.timestamp(), end_time.timestamp(), batch_size, offset),
                )
                batch = [InvocationId(inv_id) for (inv_id,) in cursor.fetchall()]
                cursor.close()

            if not batch:
                break

            yield batch
            offset += batch_size

    def iter_history_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100,
    ) -> Iterator[list[InvocationHistory]]:
        """Iterate over history entries within time range."""
        offset = 0
        while True:
            with sqlite_conn(self.sqlite_db_path) as conn:
                cursor = conn.execute(
                    f"""
                    SELECT history_json
                    FROM {Tables.HISTORY}
                    WHERE history_timestamp >= ? AND history_timestamp <= ?
                    ORDER BY history_timestamp ASC
                    LIMIT ? OFFSET ?
                """,
                    (start_time.timestamp(), end_time.timestamp(), batch_size, offset),
                )
                batch = [
                    InvocationHistory.from_json(row[0]) for row in cursor.fetchall()
                ]
                cursor.close()

            if not batch:
                break

            yield batch
            offset += batch_size

    def _store_runner_context(self, runner_context: "RunnerContext") -> None:
        """
        Store a runner context.

        :param str runner_id: The runner's unique identifier
        :param RunnerContext runner_context: The context to store
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            parent_ctx_id = None
            if runner_context.parent_ctx:
                parent_ctx_id = runner_context.parent_ctx.runner_id
            conn.execute(
                f"""REPLACE INTO {Tables.RUNNER_CONTEXTS}
                (runner_id, runner_cls, parent_ctx_id, pid, hostname, thread_id)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    runner_context.runner_id,
                    runner_context.runner_cls,
                    parent_ctx_id,
                    runner_context.pid,
                    runner_context.hostname,
                    runner_context.thread_id,
                ),
            )
            conn.commit()

    def _parse_runner_context_row(self, row: tuple) -> "RunnerContext":
        runner_id, runner_cls, parent_ctx_id, pid, hostname, thread_id = row
        parent_ctx = None
        if parent_ctx_id:
            parent_ctx = self.get_runner_context(parent_ctx_id)
            if not parent_ctx:
                raise KeyError(f"Parent RunnerContext {parent_ctx_id} not found")
        return RunnerContext(
            runner_cls=runner_cls,
            runner_id=runner_id,
            parent_ctx=parent_ctx,
            pid=pid,
            hostname=hostname,
            thread_id=thread_id,
        )

    def _get_runner_context(self, runner_id: str) -> "RunnerContext | None":
        """
        Retrieve a runner context by runner_id from SQLite.

        :param str runner_id: The runner's unique identifier
        :return: The stored RunnerContext or None if not found
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cur = conn.execute(
                f"""SELECT runner_cls, parent_ctx_id, pid, hostname, thread_id
                FROM {Tables.RUNNER_CONTEXTS}
                WHERE runner_id = ?""",
                (runner_id,),
            )
            row = cur.fetchone()
            cur.close()
            if row:
                return self._parse_runner_context_row((runner_id, *row))
            return None

    def _get_runner_contexts(self, runner_ids: list[str]) -> list["RunnerContext"]:
        """
        Retrieve multiple runner contexts by their IDs.

        :param list[str] runner_ids: List of runner unique identifiers
        :return: list["RunnerContext"] of the stored RunnerContexts
        """
        contexts = []
        with sqlite_conn(self.sqlite_db_path) as conn:
            cur = conn.execute(
                f"""SELECT runner_id, runner_cls, parent_ctx_id, pid, hostname, thread_id
                FROM {Tables.RUNNER_CONTEXTS}
                WHERE runner_id IN ({",".join(["?"] * len(runner_ids))})""",
                tuple(runner_ids),
            )
            for row in cur.fetchall():
                contexts.append(self._parse_runner_context_row(row))
            cur.close()
        return contexts

    def purge(self) -> None:
        """Clear all state backend data"""
        delete_tables_with_prefix(self.sqlite_db_path, "state_backend")
        init_tables(self.sqlite_db_path)
