"""
SQLite-based orchestrator for cross-process testing.

This module provides a SQLite-based orchestrator implementation that enables
true cross-process coordination for testing process runners. Unlike shared memory,
SQLite provides ACID transactions and handles concurrent access automatically.
"""

import sqlite3
import json
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from functools import cached_property
from time import time
from typing import TYPE_CHECKING

from pynenc.conf.config_orchestrator import ConfigOrchestratorSQLite
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.invocation.status import (
    InvocationStatus,
    InvocationStatusRecord,
    status_record_transition,
)
from pynenc.orchestrator.base_orchestrator import (
    BaseBlockingControl,
    BaseOrchestrator,
)
from pynenc.orchestrator.atomic_service import (
    ActiveRunnerInfo,
    AtomicServiceExecution,
    AtomicServiceExecutionStatus,
)
from pynenc.util.sqlite_utils import TableNames
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import (
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.identifiers.call_id import CallId
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.orchestrator.atomic_service import AtomicServiceRun
    from pynenc.task import Task, TaskId
    from pynenc.types import Params, Result
    from pynenc.util.sqlite_utils import SQLiteConnection


class Tables(TableNames):
    """Table names for orchestrator, scoped by app_id."""

    def __init__(self, app_id: str) -> None:
        super().__init__(app_id, "orchestrator")
        p = self.table_prefix
        self.INVOCATIONS = f"{p}_invocations"
        self.INVOCATION_ARGS = f"{p}_invocation_args"
        self.BLOCKING_EDGES = f"{p}_blocking_edges"
        self.RUNNER_HEARTBEATS = f"{p}_runner_heartbeats"
        self.ATOMIC_SERVICE_EXECUTIONS = f"{p}_atomic_service_executions"


class SQLiteBlockingControl(BaseBlockingControl):
    """
    Blocking control for SQLiteOrchestrator using SQLite for cross-process invocation dependencies.

    This class manages dependencies between task invocations, ensuring that invocations waiting for others
    are properly tracked and released. Implements blocking control using persistent SQLite tables.

    Key components:
    - waiting_for: Tracks which invocations are waiting for results from others
    - waited_by: Tracks which invocations are being waited on by others
    """

    def __init__(self, app: "Pynenc", sqlite_db_path: str, tables: Tables) -> None:
        self.app = app
        self.sqlite_db_path = sqlite_db_path
        self.tables = tables
        self._init_tables()

    def _init_tables(self) -> None:
        """Initialize SQLite table for blocking control."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.BLOCKING_EDGES} (
                    waiter_id TEXT NOT NULL,
                    waited_id TEXT NOT NULL,
                    PRIMARY KEY (waiter_id, waited_id)
                )
            """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables.BLOCKING_EDGES}_waited_id ON {self.tables.BLOCKING_EDGES}(waited_id)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables.BLOCKING_EDGES}_waiter_id ON {self.tables.BLOCKING_EDGES}(waiter_id)"
            )
            conn.commit()

    def waiting_for_results(
        self,
        caller_invocation_id: "InvocationId",
        result_invocation_ids: list["InvocationId"],
    ) -> None:
        """Notifies the system that an invocation is waiting for the results of other invocations."""
        waiter_id = caller_invocation_id
        with sqlite_conn(self.sqlite_db_path) as conn:
            for waited_id in result_invocation_ids:
                conn.execute(
                    f"INSERT OR IGNORE INTO {self.tables.BLOCKING_EDGES} (waiter_id, waited_id) VALUES (?, ?)",
                    (waiter_id, waited_id),
                )
                conn.execute(
                    f"INSERT OR IGNORE INTO {self.tables.BLOCKING_EDGES} (waiter_id, waited_id) VALUES (?, ?)",
                    (waiter_id, waited_id),
                )
            conn.commit()

    def release_waiters(self, waited_invocation_id: str) -> None:
        """Removes an invocation from the graph, along with any dependencies related to it."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"DELETE FROM {self.tables.BLOCKING_EDGES} WHERE waited_id = ?",
                (waited_invocation_id,),
            )
            conn.commit()

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["InvocationId"]:
        """Retrieves invocations that are blocking others but are not themselves waiting for any results."""
        available_statuses = tuple(
            status.value for status in InvocationStatus.get_available_for_run_statuses()
        )
        placeholders = ",".join("?" for _ in available_statuses)
        query = f"""
            SELECT DISTINCT b.waited_id
            FROM {self.tables.BLOCKING_EDGES} b
            JOIN {self.tables.INVOCATIONS} i ON b.waited_id = i.invocation_id
            WHERE b.waited_id NOT IN (
                SELECT waiter_id FROM {self.tables.BLOCKING_EDGES}
            )
            AND i.status IN ({placeholders})
            LIMIT ?
        """
        params = [*available_statuses, max_num_invocations]
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(query, tuple(params))
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (waited_id,) in cursor_rows:
                yield waited_id


class SQLiteOrchestrator(BaseOrchestrator):
    """
    A SQLite-based implementation of the orchestrator for cross-process testing.

    This orchestrator uses SQLite for cross-process coordination and implements
    all required abstract methods from BaseOrchestrator. It's designed specifically
    for testing process runners.

    ```{warning}
    The `SQLiteOrchestrator` class is designed for testing purposes only and should
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

        # Initialize control components
        self._blocking_control = SQLiteBlockingControl(
            app, self.sqlite_db_path, self.tables
        )

    def _init_tables(self) -> None:
        """Initialize SQLite tables for orchestrator state."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.INVOCATIONS} (
                    invocation_id TEXT PRIMARY KEY,
                    task_id_key TEXT NOT NULL,
                    call_id_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    status_runner_id TEXT,
                    status_timestamp REAL NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    auto_purge_timestamp REAL
                )
            """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables.INVOCATIONS}_task_id ON {self.tables.INVOCATIONS}(task_id_key)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables.INVOCATIONS}_call_id ON {self.tables.INVOCATIONS}(call_id_key)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables.INVOCATIONS}_status ON {self.tables.INVOCATIONS}(status)"
            )

            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.INVOCATION_ARGS} (
                    invocation_id TEXT NOT NULL,
                    arg_key TEXT NOT NULL,
                    arg_value TEXT NOT NULL,
                    PRIMARY KEY (invocation_id, arg_key)
                )
            """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.tables.INVOCATION_ARGS}_key_value ON {self.tables.INVOCATION_ARGS}(arg_key, arg_value)
            """
            )

            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.RUNNER_HEARTBEATS} (
                    runner_id TEXT PRIMARY KEY,
                    creation_timestamp REAL NOT NULL,
                    allow_to_run_atomic_service INTEGER NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    consumed_queues TEXT NOT NULL DEFAULT '[]'
                )
            """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables.RUNNER_HEARTBEATS}_heartbeat ON {self.tables.RUNNER_HEARTBEATS}(last_heartbeat)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables.RUNNER_HEARTBEATS}_creation ON {self.tables.RUNNER_HEARTBEATS}(creation_timestamp)"
            )

            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.ATOMIC_SERVICE_EXECUTIONS} (
                    runner_id TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    atomic_service_run_id TEXT NOT NULL PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'running',
                    reason TEXT NOT NULL DEFAULT ''
                )
            """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.tables.ATOMIC_SERVICE_EXECUTIONS}_time
                ON {self.tables.ATOMIC_SERVICE_EXECUTIONS}(start_time, end_time)
                """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.tables.ATOMIC_SERVICE_EXECUTIONS}_status
                ON {self.tables.ATOMIC_SERVICE_EXECUTIONS}(status)
                """
            )
            try:
                conn.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS
                    idx_{self.tables.ATOMIC_SERVICE_EXECUTIONS}_single_running
                    ON {self.tables.ATOMIC_SERVICE_EXECUTIONS}(status)
                    WHERE status = 'running'
                    """
                )
            except sqlite3.IntegrityError:
                self._repair_duplicate_atomic_service_running_rows(conn)
                conn.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS
                    idx_{self.tables.ATOMIC_SERVICE_EXECUTIONS}_single_running
                    ON {self.tables.ATOMIC_SERVICE_EXECUTIONS}(status)
                    WHERE status = 'running'
                    """
                )

            conn.commit()

    def _repair_duplicate_atomic_service_running_rows(
        self, conn: "SQLiteConnection"
    ) -> None:
        """Keep the newest RUNNING row so the single-active index can be created."""
        table = self.tables.ATOMIC_SERVICE_EXECUTIONS
        conn.execute(
            f"""
            UPDATE {table}
            SET end_time = start_time,
                status = ?,
                reason = CASE
                    WHEN reason = '' THEN ?
                    ELSE reason
                END
            WHERE status = ?
              AND atomic_service_run_id NOT IN (
                  SELECT atomic_service_run_id
                  FROM {table}
                  WHERE status = ?
                  ORDER BY start_time DESC
                  LIMIT 1
              )
            """,
            (
                str(AtomicServiceExecutionStatus.ABANDONED),
                "recovered_duplicate_running",
                str(AtomicServiceExecutionStatus.RUNNING),
                str(AtomicServiceExecutionStatus.RUNNING),
            ),
        )

    @cached_property
    def conf(self) -> ConfigOrchestratorSQLite:
        return ConfigOrchestratorSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def blocking_control(self) -> BaseBlockingControl:
        """Return blocking control."""
        return self._blocking_control

    def _register_new_invocations(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """Register new invocations with status Register if they don't exist yet."""
        status_record = InvocationStatusRecord(InvocationStatus.REGISTERED, runner_id)
        with sqlite_conn(self.sqlite_db_path) as conn:
            for invocation in invocations:
                conn.execute(
                    f"""
                    INSERT INTO {self.tables.INVOCATIONS} (
                    invocation_id, task_id_key, call_id_key, status, status_runner_id, status_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(invocation_id) DO NOTHING
                """,
                    (
                        invocation.invocation_id,
                        invocation.task.task_id.key,
                        invocation.call.call_id.key,
                        status_record.status.value,
                        status_record.runner_id,
                        status_record.timestamp.timestamp(),
                    ),
                )
            conn.commit()
        return status_record

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> Iterator["InvocationId"]:
        """
        Get existing invocation IDs for a task, optionally filtered by arguments and statuses.

        :param Task[Params, Result] task: The task for which to retrieve invocation IDs.
        :param dict[str, str] | None key_serialized_arguments: Serialized arguments to filter invocations.
        :param list[InvocationStatus] | None statuses: The statuses to filter invocations.
        :return: An iterator over matching invocation IDs.
        """
        query = f"SELECT i.invocation_id FROM {self.tables.INVOCATIONS} i"
        joins = []
        params = []
        if key_serialized_arguments:
            idx = 0
            for k, v in key_serialized_arguments.items():
                alias = f"a{idx}"
                joins.append(
                    f"JOIN {self.tables.INVOCATION_ARGS} {alias} ON i.invocation_id = {alias}.invocation_id AND {alias}.arg_key = ? AND {alias}.arg_value = ?"
                )
                params.extend([k, v])
                idx += 1
        wheres = ["i.task_id_key = ?"]
        params.append(task.task_id.key)
        if statuses:
            wheres.append(f"i.status IN ({','.join(['?' for _ in statuses])})")
            params.extend([s.value for s in statuses])
        sql = query + " " + " ".join(joins)
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(sql, tuple(params))
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def get_task_invocation_ids(self, task_id: "TaskId") -> Iterator["InvocationId"]:
        """Retrieves all invocation ids for a given task id."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_id FROM {self.tables.INVOCATIONS} WHERE task_id_key = ?",
                (task_id.key,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def get_invocation_ids_paginated(
        self,
        task_id: "TaskId | None" = None,
        statuses: list[InvocationStatus] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["InvocationId"]:
        """
        Retrieves invocation IDs with pagination support.

        :param TaskId | None task_id: Optional task ID to filter by.
        :param list[InvocationStatus] | None statuses: Optional statuses to filter by.
        :param int limit: Maximum number of results to return.
        :param int offset: Number of results to skip.
        :return: List of matching invocation IDs.
        """
        query = f"SELECT invocation_id FROM {self.tables.INVOCATIONS}"
        wheres = []
        params: list = []

        if task_id:
            wheres.append("task_id_key = ?")
            params.append(task_id.key)

        if statuses:
            wheres.append(f"status IN ({','.join(['?' for _ in statuses])})")
            params.extend([s.value for s in statuses])

        sql = query
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)

        # Order by timestamp descending (newest first) for consistent pagination
        sql += " ORDER BY status_timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(sql, tuple(params))
            result = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return result

    def count_invocations(
        self,
        task_id: "TaskId | None" = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> int:
        """
        Counts invocations matching the given filters.

        :param str | None task_id: Optional task ID to filter by.
        :param list[InvocationStatus] | None statuses: Optional statuses to filter by.
        :return: The total count of matching invocations.
        """
        query = f"SELECT COUNT(*) FROM {self.tables.INVOCATIONS}"
        wheres = []
        params: list = []

        if task_id:
            wheres.append("task_id_key = ?")
            params.append(task_id.key)

        if statuses:
            wheres.append(f"status IN ({','.join(['?' for _ in statuses])})")
            params.extend([s.value for s in statuses])

        sql = query
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(sql, tuple(params))
            count = cursor.fetchone()[0]
            cursor.close()
            return count

    def get_call_invocation_ids(self, call_id: "CallId") -> Iterator["InvocationId"]:
        """Retrieves all invocation ids for a given call id."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_id FROM {self.tables.INVOCATIONS} WHERE call_id_key = ?",
                (call_id.key,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def _atomic_status_transition(
        self,
        invocation_id: "InvocationId",
        status: InvocationStatus,
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """Atomically read, validate, and write invocation status.

        Uses ``BEGIN IMMEDIATE`` to acquire a write lock before reading so that
        no two processes can concurrently observe the same "from" status, compute
        independent (valid) transitions, and both commit — which would produce
        duplicate history records and could allow forbidden status regressions
        (e.g. RUNNING → PENDING) to slip through.
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            # Acquire write lock immediately so the read-validate-write is
            # truly atomic across all concurrent SQLite connections.
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                f"SELECT status, status_runner_id, status_timestamp FROM {self.tables.INVOCATIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            if not row:
                # Raising here lets the context manager's __exit__ rollback the
                # IMMEDIATE transaction automatically.
                raise KeyError(f"Invocation ID {invocation_id} not found")
            prev_status_record = InvocationStatusRecord(
                InvocationStatus(row[0]), row[1], row[2]
            )
            new_record = status_record_transition(prev_status_record, status, runner_id)

            conn.execute(
                f"""UPDATE {self.tables.INVOCATIONS}
                        SET status = ?,
                            status_runner_id = ?,
                            status_timestamp = ?
                        WHERE invocation_id = ?""",
                (
                    new_record.status.value,
                    new_record.runner_id,
                    new_record.timestamp.timestamp(),
                    invocation_id,
                ),
            )
            conn.commit()
        return new_record

    def index_arguments_for_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            for key, value in invocation.call.serialized_arguments.items():
                conn.execute(
                    f"INSERT OR REPLACE INTO {self.tables.INVOCATION_ARGS} (invocation_id, arg_key, arg_value) VALUES (?, ?, ?)",
                    (invocation.invocation_id, key, value),
                )
            conn.commit()

    def set_up_invocation_auto_purge(self, invocation_id: str) -> None:
        """
        Set up invocation for auto-purging by setting the auto_purge_timestamp.
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"UPDATE {self.tables.INVOCATIONS} SET auto_purge_timestamp = ? WHERE invocation_id = ?",
                (time(), invocation_id),
            )
            conn.commit()

    def auto_purge(self) -> None:
        """
        Auto-purge old invocations based on auto_purge_timestamp.
        """
        threshold = time() - self.conf.auto_final_invocation_purge_hours * 3600
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_id FROM {self.tables.INVOCATIONS} WHERE auto_purge_timestamp IS NOT NULL AND auto_purge_timestamp <= ?",
                (threshold,),
            )
            to_purge = [row[0] for row in cursor.fetchall()]
            cursor.close()
            for invocation_id in to_purge:
                self.release_waiters(invocation_id)
                conn.execute(
                    f"DELETE FROM {self.tables.INVOCATIONS} WHERE invocation_id = ?",
                    (invocation_id,),
                )
                conn.execute(
                    f"DELETE FROM {self.tables.INVOCATION_ARGS} WHERE invocation_id = ?",
                    (invocation_id,),
                )
            conn.commit()

    def get_invocation_status_record(
        self, invocation_id: str
    ) -> InvocationStatusRecord:
        """
        Get the current status of an invocation by ID, handling pending timeouts.

        :param str invocation_id: The invocation ID
        :return: The current status
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""SELECT status, status_timestamp, status_runner_id
                    FROM {self.tables.INVOCATIONS}
                    WHERE invocation_id = ?""",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if not row:
                raise KeyError(f"Invocation ID {invocation_id} not found")
            status_str, status_timestamp, status_runner_id = row
            status = InvocationStatus(status_str)
            timestamp = datetime.fromtimestamp(status_timestamp, tz=UTC)
            return InvocationStatusRecord(status, status_runner_id, timestamp)

    def increment_invocation_retries(self, invocation_id: str) -> None:
        """
        Increment the retry count for an invocation by ID.

        :param str invocation_id: The invocation ID
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"UPDATE {self.tables.INVOCATIONS} SET retry_count = retry_count + 1 WHERE invocation_id = ?",
                (invocation_id,),
            )
            conn.commit()

    def get_invocation_retries(self, invocation_id: str) -> int:
        """
        Get the number of retries for an invocation by ID.

        :param str invocation_id: The invocation ID
        :return: The number of retries
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT retry_count FROM {self.tables.INVOCATIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else 0

    def filter_by_status(
        self,
        invocation_ids: list["InvocationId"],
        status_filter: frozenset["InvocationStatus"],
    ) -> list["InvocationId"]:
        """
        Filter invocations by status by ID.

        :param list["InvocationId"] invocation_ids: The invocation IDs to filter
        :param frozenset["InvocationStatus"] | None status_filter: The statuses to filter by
        :return: List of invocation IDs matching the status filter
        """
        if not invocation_ids or status_filter is None:
            return []
        with sqlite_conn(self.sqlite_db_path) as conn:
            placeholders = ",".join(["?" for _ in invocation_ids])
            status_placeholders = ",".join(["?" for _ in status_filter])
            sql = f"""
                SELECT invocation_id FROM {self.tables.INVOCATIONS}
                WHERE invocation_id IN ({placeholders}) AND status IN ({status_placeholders})
            """
            params = invocation_ids + [s.value for s in status_filter]
            cursor = conn.execute(sql, tuple(params))
            invocation_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return invocation_ids

    def register_runner_heartbeats(
        self,
        runner_ids: list[str],
        can_run_atomic_service: bool = False,
        consumed_queues: Sequence[str] | None = None,
    ) -> None:
        """Register or update heartbeat timestamps for one or more runners."""
        if not runner_ids:
            return
        current_time = time()
        queues_json = json.dumps(tuple(consumed_queues or ()))
        with sqlite_conn(self.sqlite_db_path) as conn:
            for runner_id in runner_ids:
                conn.execute(
                    f"""
                    INSERT INTO {self.tables.RUNNER_HEARTBEATS} (
                        runner_id, creation_timestamp, last_heartbeat,
                        allow_to_run_atomic_service, consumed_queues
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(runner_id) DO UPDATE SET
                        last_heartbeat = excluded.last_heartbeat,
                        allow_to_run_atomic_service = excluded.allow_to_run_atomic_service,
                        consumed_queues = excluded.consumed_queues
                    """,
                    (
                        runner_id,
                        current_time,
                        current_time,
                        int(can_run_atomic_service),
                        queues_json,
                    ),
                )
            conn.commit()

    def _get_active_runners(
        self, timeout_seconds: float, can_run_atomic_service: bool | None = None
    ) -> list[ActiveRunnerInfo]:
        """Retrieve all active runners with heartbeat information and atomic service eligibility."""
        current_time = time()
        cutoff_time = current_time - timeout_seconds

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""
                SELECT runner_id, creation_timestamp, last_heartbeat,
                       allow_to_run_atomic_service, consumed_queues
                FROM {self.tables.RUNNER_HEARTBEATS}
                WHERE last_heartbeat >= ?
                AND (? IS NULL OR allow_to_run_atomic_service = ?)
                ORDER BY creation_timestamp ASC, runner_id ASC
                """,
                (cutoff_time, can_run_atomic_service, can_run_atomic_service),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()

            active_runners = []
            for (
                runner_id,
                creation_ts,
                last_hb,
                allow_to_run_atomic_service,
                consumed_queues,
            ) in cursor_rows:
                creation_time = datetime.fromtimestamp(creation_ts, tz=UTC)
                allow = bool(allow_to_run_atomic_service)
                queues = tuple(json.loads(consumed_queues or "[]"))
                active_runners.append(
                    ActiveRunnerInfo(
                        runner_id=runner_id,
                        creation_time=creation_time,
                        last_heartbeat=datetime.fromtimestamp(last_hb, tz=UTC),
                        allow_to_run_atomic_service=allow,
                        consumed_queues=queues,
                    )
                )

            return active_runners

    def record_atomic_service_execution_start(
        self,
        atomic_service_run: "AtomicServiceRun",
        started_at: datetime | None,
        status: AtomicServiceExecutionStatus = AtomicServiceExecutionStatus.RUNNING,
        reason: str = "",
    ) -> bool:
        """Insert a new atomic-service execution record."""
        atomic_service_id = atomic_service_run.atomic_service_id
        table = self.tables.ATOMIC_SERVICE_EXECUTIONS
        running = str(AtomicServiceExecutionStatus.RUNNING)
        accepted = status != AtomicServiceExecutionStatus.BLOCKED
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if status == AtomicServiceExecutionStatus.RUNNING:
                prior = conn.execute(
                    f"""
                    SELECT runner_id, atomic_service_run_id
                    FROM {table}
                    WHERE status = ?
                    ORDER BY start_time DESC
                    LIMIT 1
                    """,
                    (running,),
                ).fetchone()
                if prior:
                    prior_runner_id, prior_run_id = prior
                    if prior_run_id == atomic_service_id.atomic_service_run_id:
                        conn.commit()
                        return True
                    actual_started_at = started_at or datetime.now(UTC)
                    atomic_service_run.started_at = actual_started_at
                    start_iso = actual_started_at.isoformat()
                    conn.execute(
                        f"""
                        INSERT OR REPLACE INTO {table}
                            (runner_id, start_time, end_time, atomic_service_run_id,
                             status, reason)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            atomic_service_id.runner_id,
                            start_iso,
                            start_iso,
                            atomic_service_id.atomic_service_run_id,
                            str(AtomicServiceExecutionStatus.BLOCKED),
                            reason
                            or (
                                f"prior_running:{prior_run_id} runner:{prior_runner_id}"
                            ),
                        ),
                    )
                    conn.commit()
                    self.purge_atomic_service_executions()
                    return False
                actual_started_at = started_at or datetime.now(UTC)
                atomic_service_run.started_at = actual_started_at
                start_iso = actual_started_at.isoformat()
                try:
                    conn.execute(
                        f"""
                        INSERT INTO {table}
                            (runner_id, start_time, end_time, atomic_service_run_id,
                             status, reason)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            atomic_service_id.runner_id,
                            start_iso,
                            None,
                            atomic_service_id.atomic_service_run_id,
                            running,
                            reason,
                        ),
                    )
                except sqlite3.IntegrityError:
                    prior = conn.execute(
                        f"""
                        SELECT runner_id, atomic_service_run_id
                        FROM {table}
                        WHERE status = ?
                        ORDER BY start_time DESC
                        LIMIT 1
                        """,
                        (running,),
                    ).fetchone()
                    prior_runner_id, prior_run_id = (
                        prior if prior else ("unknown", "unknown")
                    )
                    conn.execute(
                        f"""
                        INSERT OR REPLACE INTO {table}
                            (runner_id, start_time, end_time, atomic_service_run_id,
                             status, reason)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            atomic_service_id.runner_id,
                            start_iso,
                            start_iso,
                            atomic_service_id.atomic_service_run_id,
                            str(AtomicServiceExecutionStatus.BLOCKED),
                            reason
                            or (
                                f"prior_running:{prior_run_id} runner:{prior_runner_id}"
                            ),
                        ),
                    )
                    accepted = False
            else:
                actual_started_at = started_at or datetime.now(UTC)
                atomic_service_run.started_at = actual_started_at
                start_iso = actual_started_at.isoformat()
                end_iso: str | None = (
                    start_iso
                    if status == AtomicServiceExecutionStatus.BLOCKED
                    else None
                )
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {table}
                        (runner_id, start_time, end_time, atomic_service_run_id,
                         status, reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        atomic_service_id.runner_id,
                        start_iso,
                        end_iso,
                        atomic_service_id.atomic_service_run_id,
                        str(status),
                        reason,
                    ),
                )
            conn.commit()
        if status != AtomicServiceExecutionStatus.RUNNING or not accepted:
            self.purge_atomic_service_executions()
        return accepted

    def finalize_atomic_service_execution(
        self,
        atomic_service_run: "AtomicServiceRun",
        end_time: datetime,
        status: AtomicServiceExecutionStatus,
        reason: str = "",
    ) -> None:
        """Transition the RUNNING record for this run to a terminal status.

        Idempotent: if the row is already terminal (or absent) the UPDATE
        matches zero rows and we no-op. The caller (`_check_atomic_services`)
        only invokes this after a successful claim, so the RUNNING row exists.
        """
        atomic_service_id = atomic_service_run.atomic_service_id
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                UPDATE {self.tables.ATOMIC_SERVICE_EXECUTIONS}
                SET end_time = ?,
                    status = ?,
                    reason = CASE WHEN ? = '' THEN reason ELSE ? END
                WHERE atomic_service_run_id = ? AND status = ?
                """,
                (
                    end_time.isoformat(),
                    str(status),
                    reason,
                    reason,
                    atomic_service_id.atomic_service_run_id,
                    str(AtomicServiceExecutionStatus.RUNNING),
                ),
            )
            conn.commit()
        self.purge_atomic_service_executions()

    @staticmethod
    def _row_to_atomic_service_execution(row: tuple) -> AtomicServiceExecution:
        runner_id, start, end, run_id, status, reason = row
        return AtomicServiceExecution.from_raw(
            runner_id=runner_id,
            atomic_service_run_id=run_id,
            start_time=datetime.fromisoformat(start),
            end_time=datetime.fromisoformat(end) if end else None,
            status=AtomicServiceExecutionStatus(status),
            reason=reason,
        )

    def get_active_atomic_service_executions(
        self,
    ) -> list[AtomicServiceExecution]:
        """Return RUNNING executions ordered most-recently-started first."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT runner_id, start_time, end_time,
                       atomic_service_run_id, status, reason
                FROM {self.tables.ATOMIC_SERVICE_EXECUTIONS}
                WHERE status = ?
                ORDER BY start_time DESC
                """,
                (str(AtomicServiceExecutionStatus.RUNNING),),
            ).fetchall()
        return [self._row_to_atomic_service_execution(row) for row in rows]

    def get_atomic_service_executions_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000,
        *,
        runner_id: str | None = None,
        min_duration_seconds: float = 0.0,
    ) -> list[AtomicServiceExecution]:
        """Retrieve atomic service execution windows overlapping a time range."""
        start_iso = start_time.isoformat()
        end_iso = end_time.isoformat()
        # RUNNING rows (end_time IS NULL) are treated as "still going" and
        # overlap if their start_time falls inside the window.
        clauses = [
            "(end_time >= ? OR (end_time IS NULL AND start_time >= ?))",
            "start_time <= ?",
        ]
        params: list[object] = [start_iso, start_iso, end_iso]
        if runner_id is not None:
            clauses.append("runner_id = ?")
            params.append(runner_id)
        if min_duration_seconds > 0.0:
            clauses.append(
                "end_time IS NOT NULL AND "
                "(julianday(end_time) - julianday(start_time)) * 86400.0 >= ?"
            )
            params.append(min_duration_seconds)
        params.append(max(limit, 0))
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT runner_id, start_time, end_time, atomic_service_run_id,
                       status, reason
                FROM {self.tables.ATOMIC_SERVICE_EXECUTIONS}
                WHERE {" AND ".join(clauses)}
                ORDER BY start_time DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return [self._row_to_atomic_service_execution(row) for row in rows]

    def purge_atomic_service_executions(self) -> int:
        """Trim atomic-service execution history by age and capacity.

        Trigger-run history references atomic-service rows, so anything in
        ``protected`` is excluded from both passes to avoid dangling refs.
        RUNNING rows are always preserved — they signal a live slot holder.
        """
        retention_minutes = float(
            self.app.conf.atomic_service_execution_retention_minutes
        )
        max_records = int(self.app.conf.atomic_service_execution_max_records)
        protected = tuple(self.app.trigger.get_referenced_atomic_service_run_ids())
        # Build a `(sql_fragment, params)` pair that excludes protected rows;
        # when nothing is protected the fragment collapses to an empty AND.
        if protected:
            placeholders = ",".join("?" for _ in protected)
            protect_sql = f" AND atomic_service_run_id NOT IN ({placeholders})"
            protect_params: tuple[object, ...] = protected
        else:
            protect_sql = ""
            protect_params = ()
        table = self.tables.ATOMIC_SERVICE_EXECUTIONS
        running = str(AtomicServiceExecutionStatus.RUNNING)
        removed = 0
        with sqlite_conn(self.sqlite_db_path) as conn:
            if retention_minutes > 0:
                cutoff = (
                    datetime.now(UTC) - timedelta(minutes=retention_minutes)
                ).isoformat()
                cursor = conn.execute(
                    f"DELETE FROM {table} "
                    f"WHERE end_time IS NOT NULL AND end_time < ?" + protect_sql,
                    (cutoff, *protect_params),
                )
                removed += cursor.rowcount or 0
                cursor.close()
            if max_records > 0:
                (current_count,) = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE status != ?",
                    (running,),
                ).fetchone()
                excess = current_count - max_records
                if excess > 0:
                    cursor = conn.execute(
                        f"""
                        DELETE FROM {table}
                        WHERE atomic_service_run_id IN (
                            SELECT atomic_service_run_id FROM {table}
                            WHERE status != ?{protect_sql}
                            ORDER BY start_time ASC
                            LIMIT ?
                        )
                        """,
                        (running, *protect_params, excess),
                    )
                    removed += cursor.rowcount or 0
                    cursor.close()
            conn.commit()
        return removed

    def get_pending_invocations_for_recovery(self) -> Iterator["InvocationId"]:
        """Retrieve invocation IDs stuck in PENDING status beyond the allowed time."""
        max_pending_seconds = self.app.conf.max_pending_seconds
        current_time = time()
        cutoff_time = current_time - max_pending_seconds

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""
                SELECT invocation_id
                FROM {self.tables.INVOCATIONS}
                WHERE status = ? AND status_timestamp <= ?
                """,
                (InvocationStatus.PENDING.value, cutoff_time),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()

            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def _get_running_invocations_for_recovery(
        self, timeout_seconds: float
    ) -> Iterator["InvocationId"]:
        """Retrieve RUNNING invocation IDs owned by inactive runners."""
        current_time = time()
        cutoff_time = current_time - timeout_seconds

        with sqlite_conn(self.sqlite_db_path) as conn:
            # Find RUNNING invocations where the owner is not in active runners
            # A runner is active if it has a recent heartbeat
            cursor = conn.execute(
                f"""
                SELECT i.invocation_id
                FROM {self.tables.INVOCATIONS} i
                LEFT JOIN {self.tables.RUNNER_HEARTBEATS} r ON i.status_runner_id = r.runner_id
                WHERE i.status = ?
                  AND i.status_runner_id IS NOT NULL
                  AND (r.runner_id IS NULL OR r.last_heartbeat < ?)
                """,
                (InvocationStatus.RUNNING.value, cutoff_time),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()

            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def purge(self) -> None:
        """
        Clear all orchestrator state.
        """
        delete_tables_with_prefix(self.sqlite_db_path, self.tables.table_prefix)
        self._init_tables()
